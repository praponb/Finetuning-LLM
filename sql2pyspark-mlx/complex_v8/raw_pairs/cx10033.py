from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_is_skipped = Window.partitionBy("device", "is_skipped")
w_played_at_to_date_by_played = Window.partitionBy(F.to_date("played_at")).orderBy(F.col("played_at").desc(), "id").rowsBetween(-6, Window.currentRow)

base_plays = (
    df_plays
    .filter(F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
    )
)

ranked = (
    base_plays
    .select(
        "*",
        F.round(F.col("ms_played_bucket") - F.avg("ms_played_bucket").over(w_device_is_skipped), 2).alias("ms_played_bucket_vs_device_is_skipped_avg"),
        F.max("ms_played_bucket").over(w_played_at_to_date_by_played).alias("ms_played_bucket_max_7"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.max("ms_played").alias("max_play_ms_played"),
        F.max("played_at").alias("last_played_at"),
        F.count("*").alias("play_count"),
    )
)

summarized_rows = (
    ranked.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        "p.max_play_ms_played",
        "p.last_played_at",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

exploded_detail_plays = (
    summarized_rows.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.played_at") >= F.lit("2025-01-13 12:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played_bucket",
        "s.source_or_unknown",
        "s.ms_played_bucket_vs_device_is_skipped_avg",
        "s.max_play_ms_played",
        "s.last_played_at",
        "s.play_count",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.source").alias("play_source"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.played_at").alias("play_played_at"),
    )
)

annotated = (
    exploded_detail_plays
    .select("*", F.month(F.col("play_played_at")).alias("play_played_at_month"))
)

result = (
    annotated
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played_bucket",
        "source_or_unknown",
        "max_play_ms_played",
        "last_played_at",
        "play_count",
        "play_id",
        "play_device",
        "play_listener_id",
        "play_played_at",
        F.coalesce(F.col("play_played_at_month"), F.lit(0)).alias("play_played_at_month_filled"),
    )
    .orderBy(F.col("track_id").asc(), "id", "play_id")
    .limit(10)
)
