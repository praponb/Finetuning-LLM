from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_session_id = Window.partitionBy("agent_id", "session_id")
w_all_by_duration_min_moving_1 = Window.orderBy("duration_min", "agent_id", "session_id").rowsBetween(-1, Window.currentRow)

scoped_tickets = (
    df_tickets
    .filter(F.month(F.col("closed_at")).isin(2, 3))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "tags",
    )
)

with_prev = scoped_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_agent_id_running))

session_summary = (
    sessionized
    .groupBy("agent_id", "session_id")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

metrics = (
    session_summary
    .select(
        "agent_id",
        "session_id",
        "session_start",
        "session_end",
        "n_events",
        F.count(F.lit(1)).over(w_agent_id_session_id).alias("rows_in_agent_id_session_id"),
        F.round(F.avg("duration_min").over(w_all_by_duration_min_moving_1), 2).alias("duration_min_avg_2"),
    )
)

result = (
    metrics
    .select(
        "agent_id",
        "session_id",
        "session_start",
        "session_end",
        "rows_in_agent_id_session_id",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("session_start")).alias("days_since_session_start"),
        F.when(F.col("session_id") >= 5, "large").when(F.col("session_id") >= 3, "medium").otherwise("small").alias("session_id_tier"),
    )
    .orderBy(
        F.col("days_since_session_start").asc_nulls_last(),
        F.col("agent_id").desc(),
    )
)
