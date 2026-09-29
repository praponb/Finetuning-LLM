from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel").orderBy("opened_at", "id")
w_channel_running = Window.partitionBy("channel").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_tickets
    .filter(F.col("opened_at") >= F.lit("2025-01-25 08:00:00").cast("timestamp"))
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
        F.substring(F.trim(F.col("channel")), 1, 3).alias("channel_prefix"),
    )
)

with_prev = recent.withColumn("prev_opened_at", F.lag("opened_at").over(w_channel))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_channel_running))

sessions = (
    sessionized
    .groupBy("channel", "burst_id")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

summary = (
    sessions
    .groupBy("channel", "n_events", "burst_id")
    .agg(
        F.count("*").alias("row_count"),
        F.array_sort(F.collect_set("channel")).alias("channel_list"),
        F.countDistinct("channel").alias("distinct_channels"),
        F.round(F.stddev("n_events"), 2).alias("stddev_n_events"),
    )
    .filter(F.col("row_count") >= 3)
)

annotated = (
    summary
    .filter(F.col("n_events") < 4)
    .select(
        "*",
        (F.col("distinct_channels") - F.col("burst_id")).alias("distinct_channels_minus_burst_id"),
        F.round(F.col("burst_id") / F.nullif(F.col("stddev_n_events"), F.lit(0)), 3).alias("burst_id_per_stddev_n_events"),
        F.concat(F.col("channel"), F.lit("#"), F.col("n_events").cast("string")).alias("channel_ref"),
    )
)

result = (
    annotated
    .select(
        "channel",
        "n_events",
        "row_count",
        "channel_list",
        "distinct_channels",
        "distinct_channels_minus_burst_id",
        "burst_id_per_stddev_n_events",
        F.round(F.col("row_count") / F.nullif(F.col("stddev_n_events"), F.lit(0)), 3).alias("row_count_per_stddev_n_events"),
    )
)
