from pyspark.sql import functions as F

cleaned = (
    df_artists
    .filter(~F.col("is_verified"))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "is_verified",
        F.concat(F.col("genre"), F.lit("#"), F.col("debut_year").cast("string")).alias("genre_ref"),
    )
)

with_activity = (
    cleaned.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left_semi")
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(F.min("duration_sec").alias("min_track_duration_sec"))
)

with_counts = (
    with_activity.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select("s.*", "t.min_track_duration_sec")
)

labeled = (
    with_counts
    .filter(F.col("artist_name").isNotNull())
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("genre_ref")), " ", "_")).alias("genre_ref_slug"),
        F.concat(F.col("genre_ref"), F.lit("#"), F.col("min_track_duration_sec").cast("string")).alias("genre_ref_ref"),
        F.when(F.col("country").isin("BR", "US"), "group_a").when(F.col("country") == "TH", "group_b").otherwise("other").alias("country_group"),
    )
)

result = (
    labeled
    .select(
        "id",
        "artist_name",
        "country",
        "is_verified",
        "genre_ref",
        "genre_ref_slug",
        "genre_ref_ref",
        "country_group",
        F.substring(F.trim(F.col("genre_ref_slug")), 1, 2).alias("genre_ref_slug_prefix"),
    )
    .orderBy(F.col("country_group").desc(), F.col("genre_ref_ref").asc())
)
