from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_bpm_minus_media = Window.partitionBy("artist_id").orderBy("bpm_minus_median", "id")
w_artist_id_by_bpm_minus_media_2 = Window.partitionBy("artist_id").orderBy("bpm_minus_median", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_tracks
    .filter(F.col("release_date") < F.lit("2025-08-02").cast("date"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
        F.array_contains(F.col("moods"), "party").alias("has_party"),
        F.upper(F.trim(F.col("title"))).alias("title_clean"),
    )
)

with_parent = (
    cleaned.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.bpm",
        "s.moods",
        "s.has_party",
        "s.title_clean",
        F.col("a.is_verified").alias("artist_is_verified"),
        "a.artist_name",
    )
)

artist_id_stats = (
    with_parent
    .groupBy("artist_id")
    .agg(F.percentile_approx("bpm", 0.5).alias("median_bpm"))
)

above_average = (
    with_parent.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.bpm") >= F.col("g.median_bpm"))
    .select(
        "s.*",
        "g.median_bpm",
        F.round(F.col("s.bpm") - F.col("g.median_bpm"), 2).alias("bpm_minus_median"),
    )
)

with_windows = (
    above_average
    .select(
        "id",
        "artist_id",
        "bpm",
        "has_party",
        "artist_is_verified",
        "artist_name",
        "median_bpm",
        "bpm_minus_median",
        F.row_number().over(w_artist_id_by_bpm_minus_media).alias("bpm_minus_median_seq"),
        F.lag("bpm").over(w_artist_id_by_bpm_minus_media).alias("prev_bpm"),
        F.nth_value("median_bpm", 2).over(w_artist_id_by_bpm_minus_media_2).alias("second_median_bpm"),
    )
)

result = (
    with_windows
    .groupingSets(
        [["bpm_minus_median_seq", "artist_is_verified"], ["bpm_minus_median_seq"], ["artist_is_verified"]],
        "bpm_minus_median_seq", "artist_is_verified",
    )
    .agg(
        F.round(F.avg("prev_bpm"), 2).alias("avg_prev_bpm"),
        F.array_sort(F.collect_set("artist_name")).alias("artist_name_list"),
        F.round(100 * F.count(F.when(F.col("artist_is_verified"), 1)) / F.count("*"), 1).alias("pct_artist_is_verified_true"),
    )
    .orderBy(
        F.col("bpm_minus_median_seq").asc_nulls_last(),
        F.col("artist_is_verified").asc_nulls_last(),
    )
)
