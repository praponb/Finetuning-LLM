from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_event_type = Window.partitionBy("actor", "event_type")
w_all_by_starts_session_prior = Window.orderBy(F.col("starts_session").desc(), "id").rowsBetween(-2, -1)
w_all_by_starts_session = Window.orderBy(F.col("starts_session").desc(), "id")

valid = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.dayofweek(F.col("event_at")).alias("event_at_dayofweek"),
    )
)

with_prev = valid.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_actor_running))

metrics = (
    sessionized
    .select(
        "*",
        F.count(F.lit(1)).over(w_actor_event_type).alias("rows_in_actor_event_type"),
        F.max("event_at_dayofweek").over(w_all_by_starts_session_prior).alias("event_at_dayofweek_max_2_prior"),
        F.row_number().over(w_all_by_starts_session).alias("seq_no"),
    )
)

result = (
    metrics
    .select(
        "id",
        F.col("seq_no").alias("seq_no_v"),
        F.col("session_no").alias("m_session_no"),
    )
    .unpivot(
        ids=["id"],
        values=["seq_no_v", "m_session_no"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "field")
)
