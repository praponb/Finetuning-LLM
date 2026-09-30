from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel").orderBy("opened_at", "id")
w_channel_running = Window.partitionBy("channel").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        F.when(F.col("requester_id") >= 21, "A").when(F.col("requester_id") >= 5, "B").otherwise("C").alias("requester_id_tier"),
    )
)

with_prev = base_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_channel))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_channel_running))

sessions = (
    sessionized
    .groupBy("channel", "burst_id")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("requester_id").alias("session_requester_id"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

tagged = (
    sessions
    .filter(F.col("events") <= 5)
    .select(
        "channel",
        "burst_id",
        "session_end",
        "session_requester_id",
        "duration_min",
        (F.floor(F.col("duration_min") / 50) * 50).alias("duration_min_bucket"),
    )
)

result = (
    tagged
    .select(
        "channel",
        "burst_id",
        "session_end",
        "session_requester_id",
        "duration_min",
        "duration_min_bucket",
    )
)
