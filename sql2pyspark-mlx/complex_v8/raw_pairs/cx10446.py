from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel").orderBy("opened_at", "id")
w_channel_running = Window.partitionBy("channel").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_tickets
    .filter(F.col("requester_id") >= 5)
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.initcap(F.trim(F.col("priority"))).alias("priority_clean"),
    )
)

with_prev = cleaned.withColumn("prev_opened_at", F.lag("opened_at").over(w_channel))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_channel_running))

per_session = (
    sessionized
    .groupBy("channel", "session_no")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

screened = (
    per_session
    .crossJoin(per_session.agg(F.avg("event_count").alias("overall_avg_event_count")))
    .filter(F.col("event_count") > F.col("overall_avg_event_count"))
    .drop("overall_avg_event_count")
)

scored = (
    screened
    .filter(F.col("session_no") > 2)
    .select(
        "channel",
        "session_no",
        "session_start",
        "session_end",
        "event_count",
        "duration_min",
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug"),
        F.when(F.col("duration_min") >= 144.0, "HIGH").when(F.col("duration_min") >= 108.0, "MEDIUM").otherwise("LOW").alias("duration_min_tier"),
    )
)

result = (
    scored
    .filter(F.col("event_count") >= 2)
    .select("duration_min")
    .distinct()
    .orderBy(F.col("duration_min").asc_nulls_last())
)
