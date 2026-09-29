from pyspark.sql import functions as F

base_artists = (
    df_artists
    .filter(F.col("debut_year") >= 1994)
    .select(
        "id",
        "genre",
        "country",
        "is_verified",
        F.lower(F.trim(F.col("country"))).alias("country_clean"),
        F.lower(F.trim(F.col("artist_name"))).alias("artist_name_clean"),
        F.coalesce(F.col("genre"), F.lit("UNKNOWN")).alias("genre_or_unknown"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(F.min("release_date").alias("first_track_release_date"))
)

profiled = (
    base_artists.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select("s.*", "t.first_track_release_date")
)

track_stats_2 = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.sum("bpm").alias("total_track_bpm"),
        F.count("*").alias("track_count"),
        F.max("release_date").alias("last_track_release_date"),
    )
)

with_counts = (
    profiled.alias("s")
    .join(track_stats_2.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.total_track_bpm",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
        "t.last_track_release_date",
    )
)

result = (
    with_counts
    .select(
        "id",
        "is_verified",
        "artist_name_clean",
        "genre_or_unknown",
        "first_track_release_date",
        "total_track_bpm",
        "track_count",
        F.coalesce(F.col("total_track_bpm"), F.lit(0)).alias("total_track_bpm_filled"),
        F.concat(F.col("genre"), F.lit("#"), F.col("id").cast("string")).alias("genre_ref"),
    )
    .orderBy("artist_name_clean", F.col("id").desc())
)
