from pyspark.sql import functions as F

recent = (
    df_artists
    .filter(F.col("debut_year") > 1997)
    .select(
        "id",
        "genre",
        "debut_year",
        "is_verified",
    )
)

qualified = (
    recent
    .crossJoin(recent.agg(F.avg("debut_year").alias("overall_avg_debut_year")))
    .filter(F.col("debut_year") > F.col("overall_avg_debut_year"))
    .drop("overall_avg_debut_year")
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.max("release_date").alias("last_track_release_date"),
        F.min("release_date").alias("first_track_release_date"),
        F.round(F.avg("duration_sec"), 2).alias("avg_track_duration_sec"),
    )
)

with_counts = (
    qualified.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.last_track_release_date",
        "t.first_track_release_date",
        "t.avg_track_duration_sec",
    )
)

tagged = (
    with_counts
    .filter(F.col("debut_year") >= 1991)
    .withColumn("debut_year_distance", F.abs(F.col("debut_year") - 2015))
)

result = (
    tagged
    .groupBy("first_track_release_date")
    .pivot("genre", ["Classical", "Pop", "Indie"])
    .agg(F.min("avg_track_duration_sec"))
    .orderBy("first_track_release_date")
)
