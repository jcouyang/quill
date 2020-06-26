package io.getquill

import com.datastax.driver.core._
import io.getquill.util.ContextLogger
import cats.effect._
import cats.implicits._
import scala.concurrent.{ ExecutionContext, Future }
import io.getquill.context.cassandra.CassandraSessionContext
import io.getquill.context.cassandra.util.FutureConversions._
import scala.jdk.CollectionConverters._

class CassandraCatsEffectContext[N <: NamingStrategy](
  val naming: N,
  session:    Session
) extends CassandraSessionContext[N] {
  override type Result[T] = IO[T]
  override type RunQuerySingleResult[T] = Option[T]
  override type RunQueryResult[T] = Seq[T]
  override type RunActionResult = Unit
  override type RunBatchActionResult = Unit
  override type Session = com.datastax.driver.core.Session
  override def prepareAsync(cql: String)(implicit executionContext: ExecutionContext): Future[BoundStatement] = {
    Future(session.prepare(cql).bind())
  }

  override def close() = {
    import scala.concurrent.ExecutionContext.Implicits.global
    session.close()
  }

  private val logger = ContextLogger(this.getClass)

  def prepareAction[T](cql: String, prepare: Prepare = identityPrepare)(implicit executionContext: ExecutionContext): Session => Result[BoundStatement] = (session: Session) => {
    val prepareResult = IO(prepare(session.prepare(cql).bind()))
    val preparedRow = prepareResult.map {
      case (params, bs) =>
        logger.logQuery(cql, params)
        bs
    }
    preparedRow
  }

  def prepareBatchAction[T](groups: List[BatchGroup])(implicit executionContext: ExecutionContext): Session => Result[List[BoundStatement]] = (session: Session) => {
    val batches = groups.flatMap {
      case BatchGroup(cql, prepares) =>
        prepares.map(cql -> _)
    }
    batches.traverse {
      case (cql, prepare) =>
        val prepareCql = prepareAction(cql, prepare)
        prepareCql(session)
    }
  }

  def executeQuery[T](cql: String, prepare: Prepare = identityPrepare, extractor: Extractor[T] = identityExtractor)(implicit executionContext: ExecutionContext): Result[RunQueryResult[T]] = {
    implicit val contextShift: ContextShift[IO] = IO.contextShift(executionContext)
    val statement = prepareAsyncAndGetStatement(cql, prepare, logger)
    val res = statement.flatMap(session.executeAsync(_).asScalaWithDefaultGlobal)
    IO.fromFuture(IO(res.map(_.all.asScala.toSeq.map(extractor.apply))))
  }

  def executeQuerySingle[T](cql: String, prepare: Prepare = identityPrepare, extractor: Extractor[T] = identityExtractor)(implicit executionContext: ExecutionContext): Result[RunQuerySingleResult[T]] = {
    executeQuery(cql, prepare, extractor).map(_.headOption)
  }

  def executeAction[T](cql: String, prepare: Prepare = identityPrepare)(implicit executionContext: ExecutionContext): Result[RunActionResult] = {
    val statement = prepareAsyncAndGetStatement(cql, prepare, logger)
    implicit val contextShift: ContextShift[IO] = IO.contextShift(executionContext)
    val res = statement.flatMap(session.executeAsync(_).asScalaWithDefaultGlobal)
    IO.fromFuture(IO(res)).void
  }

  def executeBatchAction(groups: List[BatchGroup])(implicit executionContext: ExecutionContext): Result[RunBatchActionResult] = {
    groups.flatMap {
      case BatchGroup(cql, prepares) =>
        prepares.map(executeAction(cql, _))
    }.sequence_
  }
}
