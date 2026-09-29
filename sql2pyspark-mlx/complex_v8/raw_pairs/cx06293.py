from pyspark.sql import functions as F

filtered_plays = (
    df_plays
    .filter((F.col("ms_played") <= 308104) & (F.col("played_at") < F.lit("2025-02-12 11:00:00").cast("timestamp")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count("*").alias("play_count"),
        F.count(F.when(F.col("is_skipped"), 1)).alias("play_is_skipped_count"),
        F.count(F.when(~F.col("device").isin("desktop", "phone"), 1)).alias("play_device_count"),
    )
)

with_counts = (
    filtered_plays.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        F.coalesce(F.col("p.play_is_skipped_count"), F.lit(0)).alias("play_is_skipped_count"),
        F.coalesce(F.col("p.play_device_count"), F.lit(0)).alias("play_device_count"),
    )
)

summary = (
    with_counts
    .filter(F.hour(F.col("played_at")).between(9, 11))
    .groupBy("listener_id", "play_device_count", "play_count")
    .agg(
        F.round(F.stddev("play_is_skipped_count"), 2).alias("stddev_play_is_skipped_count"),
        F.max("played_at").alias("last_played_at"),
        F.count("*").alias("cnt"),
        F.countDistinct("device").alias("distinct_devices"),
    )
)

with_details = (
    summary.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.listener_id",
        "s.play_device_count",
        "s.play_count",
        "s.stddev_play_is_skipped_count",
        "s.last_played_at",
        "s.cnt",
        F.col("l.signup_date").alias("listener_signup_date"),
    )
)

result = (
    with_details
    .select(
        "listener_id",
        "play_device_count",
        "play_count",
        "last_played_at",
        "listener_signup_date",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("last_played_at")).alias("days_since_last_played_at"),
        F.to_date(F.col("last_played_at")).alias("last_played_at_date"),
    )
    .orderBy(F.col("listener_id").asc(), "last_played_at_date")
)
