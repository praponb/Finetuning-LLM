from pyspark.sql import functions as F

scoped = (
    df_listeners
    .filter(~F.col("country").isin("TH", "KR"))
    .select(
        "id",
        "username",
        "signup_date",
        "birth_year",
    )
)

exploded_detail_plays = (
    scoped.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.signup_date",
        "s.birth_year",
        F.col("p.id").alias("play_id"),
        F.col("p.track_id").alias("play_track_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(F.count("*").alias("play_count"))
)

summarized_rows = (
    exploded_detail_plays.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select("s.*", F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"))
)

play_track_id_stats = (
    summarized_rows
    .groupBy("play_track_id")
    .agg(F.percentile_approx("play_ms_played", 0.5).alias("median_play_ms_played"))
)

compared = (
    summarized_rows.alias("s")
    .join(play_track_id_stats.alias("g"), F.col("g.play_track_id") == F.col("s.play_track_id"), "inner")
    .filter(F.col("s.play_ms_played") >= F.col("g.median_play_ms_played"))
    .select(
        "s.*",
        "g.median_play_ms_played",
        F.round(F.col("s.play_ms_played") - F.col("g.median_play_ms_played"), 2).alias("play_ms_played_minus_median"),
    )
)

result = (
    compared
    .filter(F.col("median_play_ms_played") <= 242000.0)
    .select(
        "id",
        "signup_date",
        "birth_year",
        "play_ms_played",
        "play_count",
        "median_play_ms_played",
        "play_ms_played_minus_median",
    )
    .orderBy(F.col("median_play_ms_played").desc_nulls_first())
)
