from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_total_burst_id_runnin = Window.orderBy("total_burst_id", "events", "burst_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_total_burst_id = Window.orderBy("total_burst_id", "events", "burst_id")

valid_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "csat",
    )
)

with_prev = valid_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_agent_id_running))

session_summary = (
    sessionized
    .groupBy("agent_id", "burst_id")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

stats = (
    session_summary
    .groupBy("events", "burst_id")
    .agg(
        F.sum("burst_id").alias("total_burst_id"),
        F.avg("events").alias("avg_events_check"),
    )
    .filter(F.col("avg_events_check") > 2)
    .drop("avg_events_check")
)

sequenced = (
    stats
    .select(
        "events",
        "burst_id",
        "total_burst_id",
        F.count(F.lit(1)).over(w_all_by_total_burst_id_runnin).alias("running_count"),
        F.lag("total_burst_id", 3).over(w_all_by_total_burst_id).alias("prev3_total_burst_id"),
    )
)

result = (
    sequenced
    .select(
        "events",
        "burst_id",
        "total_burst_id",
        "running_count",
        "prev3_total_burst_id",
    )
    .orderBy(F.col("burst_id").desc(), "total_burst_id", "events")
    .limit(10)
)
