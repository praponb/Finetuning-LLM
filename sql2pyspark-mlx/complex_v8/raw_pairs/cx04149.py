from pyspark.sql import functions as F

valid_plays = (
    df_plays
    .filter((F.col("track_id") <= 24) & (~F.col("source").isin("radio", "search")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.hour(F.col("played_at")).alias("played_at_hour"),
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
)

detail_rows = (
    valid_plays.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.device") != "phone"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.is_skipped",
        "s.ms_played_bucket",
        F.col("p.id").alias("play_id"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.device").alias("play_device"),
    )
)

result = (
    detail_rows
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played_bucket",
        "play_id",
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
        (F.floor(F.col("ms_played_bucket") / 50000) * 50000).alias("ms_played_bucket_bucket"),
    )
    .orderBy("id", "play_id")
    .limit(5)
)
