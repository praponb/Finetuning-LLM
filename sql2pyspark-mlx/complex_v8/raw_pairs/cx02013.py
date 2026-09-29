from pyspark.sql import functions as F

filtered_plays = (
    df_plays
    .filter((F.col("is_skipped")) & (F.col("track_id") <= 17))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.lower(F.trim(F.col("source"))).alias("source_clean"),
        F.to_date(F.col("played_at")).alias("played_at_date"),
    )
)

detail_rows_plays = (
    filtered_plays.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.source_clean",
        "s.played_at_date",
        F.col("p.id").alias("play_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.device").alias("play_device"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

result = (
    detail_rows_plays
    .groupingSets(
        [["device", "play_is_skipped"], ["device"], ["play_is_skipped"], []],
        "device", "play_is_skipped",
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.round(100 * F.count(F.when(F.col("play_is_skipped"), 1)) / F.count("*"), 1).alias("pct_play_is_skipped_true"),
        F.countDistinct("source").alias("distinct_sources"),
    )
)
