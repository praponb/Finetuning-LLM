from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_play_is_skipped_by_total_bpm = Window.partitionBy("play_is_skipped").orderBy("total_bpm")
w_total_bpm_tier = Window.partitionBy("total_bpm_tier")

cleaned_tracks = (
    df_tracks
    .filter((F.col("artist_id").isNotNull()) & (F.lower(F.col("title")).like("%he%")))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        F.concat(F.col("title"), F.lit("#"), F.col("bpm").cast("string")).alias("title_ref"),
    )
)

detail_rows_plays = (
    cleaned_tracks.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.bpm",
        "s.moods",
        F.col("p.id").alias("play_id"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.played_at").alias("play_played_at"),
    )
)

rollup_base = (
    detail_rows_plays
    .groupBy("play_is_skipped")
    .agg(
        F.count(F.when(~F.col("play_is_skipped"), 1)).alias("n_play_is_skipped_false"),
        F.sum("bpm").alias("total_bpm"),
    )
)

labeled = (
    rollup_base
    .select(
        "play_is_skipped",
        "n_play_is_skipped_false",
        "total_bpm",
        F.round(F.col("n_play_is_skipped_false") / F.nullif(F.col("total_bpm"), F.lit(0)), 3).alias("n_play_is_skipped_false_per_total_bpm"),
        F.when(F.col("total_bpm") >= 825, "HIGH").when(F.col("total_bpm") >= 468, "MEDIUM").otherwise("LOW").alias("total_bpm_tier"),
        F.round(F.col("n_play_is_skipped_false") / F.nullif(F.col("total_bpm"), F.lit(0)), 3).alias("n_play_is_skipped_false_per_total_bpm_2"),
    )
)

tagged = (
    labeled
    .select(
        "play_is_skipped",
        "n_play_is_skipped_false",
        "total_bpm",
        "total_bpm_tier",
        "n_play_is_skipped_false_per_total_bpm_2",
        (F.col("n_play_is_skipped_false") + F.col("total_bpm")).alias("n_play_is_skipped_false_plus_total_bpm"),
        F.abs(F.col("total_bpm") - 519).alias("total_bpm_distance"),
        F.initcap(F.trim(F.col("total_bpm_tier"))).alias("total_bpm_tier_clean"),
    )
)

ranked = (
    tagged
    .select(
        "*",
        F.round(F.percent_rank().over(w_play_is_skipped_by_total_bpm), 3).alias("total_bpm_percent_rank"),
        F.round(F.col("total_bpm_distance") / F.nullif(F.sum("total_bpm_distance").over(w_total_bpm_tier), F.lit(0)), 4).alias("total_bpm_distance_share_of_total_bpm_tier"),
        F.count(F.lit(1)).over(w_total_bpm_tier).alias("rows_in_total_bpm_tier"),
    )
)

result = (
    ranked
    .select(
        "play_is_skipped",
        F.col("total_bpm_percent_rank").alias("total_bpm_percent_rank_v"),
        F.col("total_bpm_distance_share_of_total_bpm_tier").alias("total_bpm_distance_share_of_total_bpm_tier_v"),
        F.col("n_play_is_skipped_false_per_total_bpm_2").alias("n_play_is_skipped_false_per_total_bpm_2_v"),
    )
    .unpivot(
        ids=["play_is_skipped"],
        values=["total_bpm_percent_rank_v", "total_bpm_distance_share_of_total_bpm_tier_v", "n_play_is_skipped_false_per_total_bpm_2_v"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
