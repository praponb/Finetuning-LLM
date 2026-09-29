from pyspark.sql import functions as F

filtered_artists = (
    df_artists
    .filter(~F.col("is_verified"))
    .select(
        "id",
        "artist_name",
        "country",
        "debut_year",
        "is_verified",
        F.length(F.trim(F.col("artist_name"))).alias("artist_name_len"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.max("release_date").alias("last_track_release_date"),
        F.count("*").alias("track_count"),
        F.min("duration_sec").alias("min_track_duration_sec"),
    )
)

profiled = (
    filtered_artists.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.last_track_release_date",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
        "t.min_track_duration_sec",
    )
)

country_stats = (
    profiled
    .groupBy("country")
    .agg(F.percentile_approx("min_track_duration_sec", 0.5).alias("median_min_track_duration_sec"))
)

outliers = (
    profiled.alias("s")
    .join(country_stats.alias("g"), F.col("g.country") == F.col("s.country"), "inner")
    .filter(F.col("s.min_track_duration_sec") >= F.col("g.median_min_track_duration_sec"))
    .select(
        "s.*",
        "g.median_min_track_duration_sec",
        F.round(F.col("s.min_track_duration_sec") - F.col("g.median_min_track_duration_sec"), 2).alias("min_track_duration_sec_minus_median"),
    )
)

result = (
    outliers
    .select(
        "id",
        "artist_name",
        "country",
        "is_verified",
        "track_count",
        "min_track_duration_sec",
        F.date_format(F.col("last_track_release_date"), "MMM yyyy").alias("last_track_release_date_label"),
        F.lower(F.regexp_replace(F.trim(F.col("country")), " ", "_")).alias("country_slug"),
    )
)
