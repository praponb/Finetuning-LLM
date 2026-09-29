from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel").orderBy("closed_at", "id")
w_channel_running = Window.partitionBy("channel").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_tickets
    .filter(F.to_date(F.col("closed_at")) < F.lit("2025-02-20").cast("date"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.greatest(F.col("csat"), F.col("requester_id")).alias("greatest_csat_requester_id"),
    )
)

with_prev = filtered.withColumn("prev_closed_at", F.lag("closed_at").over(w_channel))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_closed_at").isNull() | ((F.unix_timestamp("closed_at") - F.unix_timestamp("prev_closed_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_channel_running))

per_session = (
    sessionized
    .groupBy("channel", "session_id")
    .agg(
        F.min("closed_at").alias("session_start"),
        F.max("closed_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("closed_at")) - F.unix_timestamp(F.min("closed_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    per_session
    .groupingSets(
        [["session_id", "channel"], ["session_id"]],
        "session_id", "channel",
    )
    .agg(F.count("*").alias("cnt"))
)
