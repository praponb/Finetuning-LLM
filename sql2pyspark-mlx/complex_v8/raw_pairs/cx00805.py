from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority").orderBy("opened_at", "id")
w_priority_running = Window.partitionBy("priority").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_tickets
    .filter(F.col("requester_id") >= 8)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

with_prev = recent.withColumn("prev_opened_at", F.lag("opened_at").over(w_priority))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_priority_running))

per_session = (
    sessionized
    .groupBy("priority", "session_id")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("csat").alias("session_csat"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

without_matches = (
    per_session
    .crossJoin(per_session.agg(F.avg("session_id").alias("overall_avg_session_id")))
    .filter(F.col("session_id") > F.col("overall_avg_session_id"))
    .drop("overall_avg_session_id")
)

annotated = (
    without_matches
    .filter(F.col("session_csat") <= 13)
    .select(
        "*",
        F.coalesce(F.col("duration_min"), F.lit(0.0)).alias("duration_min_filled"),
        F.date_trunc("day", F.col("session_start")).alias("session_start_day"),
    )
)

result = (
    annotated
    .select(
        "priority",
        "session_start",
        "session_end",
        "events",
        "session_csat",
        "duration_min",
        "duration_min_filled",
        "session_start_day",
        F.substring(F.trim(F.col("priority")), 1, 4).alias("priority_prefix"),
        F.lower(F.trim(F.col("priority"))).alias("priority_clean"),
    )
    .orderBy(F.col("session_start_day").desc(), "session_end")
)
