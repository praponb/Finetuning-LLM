from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel").orderBy("opened_at", "id")
w_channel_running = Window.partitionBy("channel").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
    )
)

with_prev = prepared.withColumn("prev_opened_at", F.lag("opened_at").over(w_channel))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_channel_running))

sessions = (
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
    sessions
    .select(
        "*",
        F.round((F.unix_timestamp(F.col("session_end")) - F.unix_timestamp(F.col("session_start"))) / 3600, 1).alias("hours_session_start_to_session_end"),
        F.when(F.col("channel").isin("email", "phone"), "group_a").when(F.col("channel") == "web", "group_b").otherwise("other").alias("channel_group"),
    )
)

derived = (
    enriched
    .select(
        "*",
        F.round(F.col("duration_min") * 1.1, 2).alias("duration_min_adj"),
        (F.floor(F.col("duration_min") / 50) * 50).alias("duration_min_bucket"),
    )
)

result = (
    derived
    .select(
        "channel",
        "burst_id",
        "session_start",
        "session_end",
        "events",
        "duration_min",
        "hours_session_start_to_session_end",
        "channel_group",
        "duration_min_adj",
        (F.floor(F.col("duration_min_adj") / 50) * 50).alias("duration_min_adj_bucket"),
    )
)
