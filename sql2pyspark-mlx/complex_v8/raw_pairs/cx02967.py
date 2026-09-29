from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")
w_all_by_duration_sec_per_bpm = Window.orderBy("duration_sec_per_bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_tracks = (
    df_tracks
    .filter((F.year(F.col("release_date")) == 2024) & (F.col("bpm").between(76, 166)))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        F.round(F.col("duration_sec") / F.nullif(F.col("bpm"), F.lit(0)), 3).alias("duration_sec_per_bpm"),
        F.initcap(F.trim(F.col("title"))).alias("title_clean"),
        F.trunc(F.col("release_date"), "MM").alias("release_date_month_start"),
    )
)

with_trends = (
    scoped_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "title_clean",
        "release_date_month_start",
        F.round(F.col("duration_sec_per_bpm") - F.avg("duration_sec_per_bpm").over(w_is_explicit_artist_id), 2).alias("duration_sec_per_bpm_vs_is_explicit_artist_id_av"),
        F.sum("duration_sec").over(w_all_by_duration_sec_per_bpm).alias("running_duration_sec"),
        F.round(F.avg("duration_sec_per_bpm").over(w_is_explicit_artist_id), 2).alias("is_explicit_artist_id_avg_duration_sec_per_bpm"),
    )
)

artist_id_stats = (
    with_trends
    .groupBy("artist_id")
    .agg(F.percentile_approx("is_explicit_artist_id_avg_duration_sec_per_bpm", 0.5).alias("median_is_explicit_artist_id_avg_duration_sec_pe"))
)

outliers = (
    with_trends.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.is_explicit_artist_id_avg_duration_sec_per_bpm") >= F.col("g.median_is_explicit_artist_id_avg_duration_sec_pe"))
    .select(
        "s.*",
        "g.median_is_explicit_artist_id_avg_duration_sec_pe",
        F.round(F.col("s.is_explicit_artist_id_avg_duration_sec_per_bpm") - F.col("g.median_is_explicit_artist_id_avg_duration_sec_pe"), 2).alias("is_explicit_artist_id_avg_duration_sec_per_bpm_m"),
    )
)

result = (
    outliers
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "release_date_month_start",
        "duration_sec_per_bpm_vs_is_explicit_artist_id_av",
        "running_duration_sec",
        "is_explicit_artist_id_avg_duration_sec_per_bpm",
        "median_is_explicit_artist_id_avg_duration_sec_pe",
        "is_explicit_artist_id_avg_duration_sec_per_bpm_m",
        F.date_trunc("week", F.col("release_date_month_start")).alias("release_date_month_start_week"),
        F.length(F.trim(F.col("title_clean"))).alias("title_clean_len"),
    )
)
