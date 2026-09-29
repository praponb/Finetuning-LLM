from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel").orderBy("opened_at", "id")
w_channel_running = Window.partitionBy("channel").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_tickets = (
    df_tickets
    .filter(F.array_contains(F.col("tags"), "outage"))
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

with_prev = recent_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_channel))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_channel_running))

session_summary = (
    sessionized
    .groupBy("channel", "burst_id")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

enriched = (
    session_summary
    .select(
        "*",
        F.year(F.col("session_end")).alias("session_end_year"),
        F.date_format(F.col("session_end"), "yyyy-MM").alias("session_end_label"),
        F.round(F.col("burst_id") / F.nullif(F.col("events"), F.lit(0)), 3).alias("burst_id_per_events"),
    )
)

result = (
    enriched
    .select(
        "channel",
        "burst_id",
        "session_start",
        "session_end",
        "events",
        "duration_min",
        "session_end_year",
        "session_end_label",
        (F.col("duration_min") - F.col("burst_id")).alias("duration_min_minus_burst_id"),
    )
    .orderBy(
        F.col("duration_min_minus_burst_id").asc(),
        F.col("duration_min").desc(),
        "channel",
        "burst_id",
    )
    .limit(5)
)
