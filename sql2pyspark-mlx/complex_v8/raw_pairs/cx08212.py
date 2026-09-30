from pyspark.sql import functions as F

valid_tracks = (
    df_tracks
    .filter((F.array_contains(F.col("moods"), "chill")) & (F.upper(F.trim(F.col("title"))).like("M%")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.abs(F.col("duration_sec") - 183).alias("duration_sec_distance"),
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
    )
)

joined = (
    valid_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.country").isin("JP", "US"))
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        "s.days_since_release_date",
        "s.bpm_bucket",
        F.col("a.country").alias("artist_country"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

derived = (
    joined
    .select(
        "*",
        F.quarter(F.col("release_date")).alias("release_date_quarter"),
        (F.col("days_since_release_date") + F.col("bpm")).alias("days_since_release_date_plus_bpm"),
        F.when(F.col("artist_is_verified"), "yes").otherwise("no").alias("artist_is_verified_label"),
    )
)

labeled = (
    derived
    .select(
        "*",
        F.when(F.col("artist_country").isin("US", "BR"), "group_a").when(F.col("artist_country") == "TH", "group_b").otherwise("other").alias("artist_country_group"),
    )
)

result = (
    labeled
    .groupingSets(
        [["artist_is_verified", "is_explicit", "artist_country"], ["artist_is_verified"]],
        "artist_is_verified", "is_explicit", "artist_country",
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.round(F.stddev("release_date_quarter"), 2).alias("stddev_release_date_quarter"),
        F.sum(F.when(~F.col("is_explicit"), 1).otherwise(0)).alias("n_is_explicit_false"),
    )
    .orderBy(
        F.col("artist_is_verified").asc_nulls_last(),
        F.col("is_explicit").asc_nulls_last(),
        F.col("artist_country").asc_nulls_last(),
    )
)
