from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_year = Window.partitionBy("release_date_year")
w_all_by_release_date_span_day = Window.orderBy("release_date_span_days")

scoped = (
    df_tracks
    .filter(F.col("duration_sec").between(144, 357))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
    )
)

line_level = (
    scoped.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

without_matches = (
    line_level.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.ms_played") >= 67932), "left_semi")
)

rollup_base = (
    without_matches
    .groupBy(F.year("release_date").alias("release_date_year"))
    .agg(
        F.min("play_ms_played").alias("min_play_ms_played"),
        F.datediff(F.max("release_date"), F.min("release_date")).alias("release_date_span_days"),
    )
)

windowed = (
    rollup_base
    .select(
        "*",
        F.count(F.lit(1)).over(w_release_date_year).alias("rows_in_release_date_year"),
        F.sum("release_date_year").over(w_release_date_year).alias("release_date_year_sum_release_date_year"),
        F.round(F.cume_dist().over(w_all_by_release_date_span_day), 3).alias("release_date_span_days_cume_dist"),
    )
)

result = (
    windowed
    .select(
        "release_date_year",
        "min_play_ms_played",
        "release_date_span_days",
        "rows_in_release_date_year",
        "release_date_span_days_cume_dist",
        F.coalesce(F.col("release_date_year"), F.lit(0)).alias("release_date_year_filled"),
    )
    .orderBy(F.col("min_play_ms_played").desc_nulls_first(), "release_date_year")
    .limit(10)
)
