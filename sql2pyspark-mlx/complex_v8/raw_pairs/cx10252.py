from pyspark.sql import functions as F

cleaned = (
    df_artists
    .filter((F.col("debut_year") > 1996) & (F.col("genre") == "HipHop"))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.concat(F.col("genre"), F.lit("#"), F.col("debut_year").cast("string")).alias("genre_ref"),
    )
)

with_children_tracks = (
    cleaned.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.artist_name",
        "s.genre",
        "s.country",
        "s.debut_year",
        "s.is_verified",
        "s.genre_ref",
        F.col("t.id").alias("track_id"),
        F.col("t.title").alias("track_title"),
        F.col("t.moods").alias("track_moods"),
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.duration_sec").alias("track_duration_sec"),
    )
)

result = (
    with_children_tracks
    .rollup("country", "track_id", "is_verified")
    .agg(F.grouping_id().alias("grouping_level"), F.count("*").alias("cnt"))
    .select(
        F.coalesce(F.col("country"), F.lit("ALL")).alias("country"),
        "track_id",
        "is_verified",
        "grouping_level",
        "cnt",
    )
    .orderBy(
        F.col("country").asc_nulls_last(),
        F.col("track_id").asc_nulls_last(),
        F.col("is_verified").asc_nulls_last(),
    )
)
