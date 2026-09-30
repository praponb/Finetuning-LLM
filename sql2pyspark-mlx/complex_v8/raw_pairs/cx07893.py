from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_play_id_play_device = Window.partitionBy("play_id", "play_device")
w_play_source_by_play_played_a = Window.partitionBy("play_source").orderBy(F.col("play_played_at").desc_nulls_first(), "id", "play_id")
w_play_id_play_source_by_durat = Window.partitionBy("play_id", "play_source").orderBy("duration_sec_bucket")

prepared = (
    df_tracks
    .filter(F.col("release_date").between(F.lit("2024-07-01").cast("date"), F.lit("2025-11-29").cast("date")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "moods",
        F.dayofweek(F.col("release_date")).alias("release_date_dayofweek"),
        F.upper(F.trim(F.col("title"))).alias("title_clean"),
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket"),
    )
)

with_children = (
    prepared.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.moods",
        "s.duration_sec_bucket",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.source").alias("play_source"),
        F.col("p.played_at").alias("play_played_at"),
    )
)

with_windows = (
    with_children
    .select(
        "id",
        "duration_sec",
        "moods",
        "duration_sec_bucket",
        "play_id",
        "play_source",
        "play_played_at",
        F.round(F.col("duration_sec_bucket") - F.avg("duration_sec_bucket").over(w_play_id_play_device), 2).alias("duration_sec_bucket_vs_play_id_play_device_avg"),
        F.lag("duration_sec_bucket").over(w_play_source_by_play_played_a).alias("prev_duration_sec_bucket"),
        F.dense_rank().over(w_play_id_play_source_by_durat).alias("duration_sec_bucket_dense_rank"),
        F.lead("play_played_at").over(w_play_source_by_play_played_a).alias("next_play_played_at"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count("*").alias("play_count"),
        F.count("*").alias("play_count_2"),
        F.max("played_at").alias("last_played_at"),
    )
)

with_stats = (
    with_windows.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        F.coalesce(F.col("p.play_count_2"), F.lit(0)).alias("play_count_2"),
        "p.last_played_at",
    )
)

qualified = (
    with_stats.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left_semi")
)

result = (
    qualified
    .filter(F.col("play_id") >= 99)
    .select(
        "id",
        "duration_sec",
        "moods",
        "duration_sec_bucket",
        "play_id",
        "play_played_at",
        "duration_sec_bucket_vs_play_id_play_device_avg",
        "duration_sec_bucket_dense_rank",
        "next_play_played_at",
        "play_count",
        "play_count_2",
        "last_played_at",
        F.when(F.col("duration_sec") >= 365, "A").when(F.col("duration_sec") >= 164, "B").otherwise("C").alias("duration_sec_tier"),
        F.when(F.col("prev_duration_sec_bucket") >= 321, "A").when(F.col("prev_duration_sec_bucket") >= 189, "B").otherwise("C").alias("prev_duration_sec_bucket_tier"),
    )
    .orderBy(F.col("id").desc())
)
