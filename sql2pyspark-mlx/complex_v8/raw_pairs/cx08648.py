from pyspark.sql import functions as F

base_tracks = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "moods",
        F.length(F.trim(F.col("title"))).alias("title_len"),
        F.year(F.col("release_date")).alias("release_date_year"),
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
    )
)

rollup_base = (
    base_tracks
    .groupBy(
        "is_explicit",
        F.year("release_date").alias("release_date_year_2"),
        "artist_id",
    )
    .agg(
        F.percentile_approx("bpm_bucket", 0.5).alias("median_bpm_bucket"),
        F.percentile_approx("title_len", 0.5).alias("median_title_len"),
        F.sum("release_date_year").alias("total_release_date_year"),
    )
)

with_details = (
    rollup_base.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.is_explicit",
        "s.release_date_year_2",
        "s.artist_id",
        "s.median_bpm_bucket",
        "s.median_title_len",
        F.col("a.genre").alias("artist_genre"),
    )
)

annotated = (
    with_details
    .filter(F.col("release_date_year_2") < 2026)
    .select(
        "is_explicit",
        "release_date_year_2",
        "artist_id",
        "median_bpm_bucket",
        "median_title_len",
        "artist_genre",
        F.coalesce(F.col("median_title_len"), F.lit(0)).alias("median_title_len_filled"),
        F.when(F.col("release_date_year_2") >= 2025, "HIGH").when(F.col("release_date_year_2") >= 2024, "MEDIUM").otherwise("LOW").alias("release_date_year_2_tier"),
    )
)

filtered = (
    annotated.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.title").isNotNull()) & (F.col("t.duration_sec") >= 213), "left_anti")
    .filter(F.col("release_date_year_2_tier") == "LOW")
)

line_level_tracks = (
    filtered.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "inner")
    .select(
        "s.is_explicit",
        "s.release_date_year_2",
        "s.artist_id",
        "s.median_title_len",
        "s.artist_genre",
        "s.median_title_len_filled",
        "s.release_date_year_2_tier",
        F.col("t.id").alias("track_id"),
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.moods").alias("track_moods"),
        F.col("t.title").alias("track_title"),
        F.col("t.release_date").alias("track_release_date"),
    )
)

release_date_year_2_tier_stats = (
    line_level_tracks
    .groupBy("release_date_year_2_tier")
    .agg(F.max("median_title_len_filled").alias("max_median_title_len_filled"))
)

vs_group = (
    line_level_tracks.alias("s")
    .join(release_date_year_2_tier_stats.alias("g"), F.col("g.release_date_year_2_tier") == F.col("s.release_date_year_2_tier"), "inner")
    .filter(F.col("s.median_title_len_filled") == F.col("g.max_median_title_len_filled"))
    .select(
        "s.*",
        "g.max_median_title_len_filled",
        F.round(F.col("s.median_title_len_filled") - F.col("g.max_median_title_len_filled"), 2).alias("median_title_len_filled_minus_max"),
    )
)

result = (
    vs_group
    .groupingSets(
        [["is_explicit", "median_title_len"], ["is_explicit"], ["median_title_len"], []],
        "is_explicit", "median_title_len",
    )
    .agg(
        F.min("track_release_date").alias("first_track_release_date"),
        F.countDistinct("artist_id").alias("distinct_artist_ids"),
        F.max("median_title_len").alias("max_median_title_len"),
    )
    .orderBy(
        F.col("is_explicit").asc_nulls_last(),
        F.col("median_title_len").asc_nulls_last(),
    )
)
