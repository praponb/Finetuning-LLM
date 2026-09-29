from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority").orderBy("closed_at", "id")
w_priority_running = Window.partitionBy("priority").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.dayofweek(F.col("opened_at")).alias("opened_at_dayofweek"),
    )
)

with_prev = filtered.withColumn("prev_closed_at", F.lag("closed_at").over(w_priority))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_closed_at").isNull() | (F.col("closed_at") > F.col("prev_closed_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_priority_running))

session_summary = (
    sessionized
    .groupBy("priority", "session_id")
    .agg(
        F.min("closed_at").alias("session_start"),
        F.max("closed_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("requester_id").alias("session_requester_id"),
        F.round((F.unix_timestamp(F.max("closed_at")) - F.unix_timestamp(F.min("closed_at"))) / 60, 1).alias("duration_min"),
    )
)

qualified = (
    session_summary
    .crossJoin(session_summary.agg(F.avg("session_requester_id").alias("overall_avg_session_requester_id")))
    .filter(F.col("session_requester_id") > F.col("overall_avg_session_requester_id"))
    .drop("overall_avg_session_requester_id")
)

result = (
    qualified
    .select(
        "priority",
        "session_id",
        "session_start",
        "event_count",
        "session_requester_id",
    )
    .orderBy(F.col("session_id").desc_nulls_first())
)
