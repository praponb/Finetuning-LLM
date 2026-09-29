from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source = Window.partitionBy("source").orderBy("played_at", "id")
w_source_running = Window.partitionBy("source").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.substring(F.trim(F.col("device")), 1, 4).alias("device_prefix"),
    )
)

with_prev = prepared_plays.withColumn("prev_played_at", F.lag("played_at").over(w_source))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_source_running))

labeled = (
    sessionized
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "is_skipped",
        "is_new_session",
        "burst_id",
        F.length(F.trim(F.col("device"))).alias("device_len"),
        F.lower(F.regexp_replace(F.trim(F.col("device_prefix")), " ", "_")).alias("device_prefix_slug"),
    )
)

result = (
    labeled
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "is_skipped",
        "is_new_session",
        "burst_id",
        "device_len",
        "device_prefix_slug",
    )
    .orderBy("device_prefix_slug", "device_len")
)
