from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel").orderBy("closed_at", "id")
w_channel_running = Window.partitionBy("channel").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_tickets
    .filter(F.col("closed_at") < F.lit("2025-03-09 04:00:00").cast("timestamp"))
    .select(
        "id",
        "agent_id",
        "closed_at",
        "channel",
        "tags",
    )
)

with_prev = cleaned.withColumn("prev_closed_at", F.lag("closed_at").over(w_channel))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_closed_at").isNull() | ((F.unix_timestamp("closed_at") - F.unix_timestamp("prev_closed_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_channel_running))

sessions = (
    sessionized
    .groupBy("channel", "burst_id")
    .agg(
        F.min("closed_at").alias("session_start"),
        F.max("closed_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("closed_at")) - F.unix_timestamp(F.min("closed_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    sessions
    .select(
        "channel",
        "burst_id",
        "session_start",
        "session_end",
        "n_events",
        "duration_min",
        (F.col("burst_id") - F.col("n_events")).alias("burst_id_minus_n_events"),
    )
)
