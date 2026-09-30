from pyspark.sql import functions as F

recent_tracks = (
    df_tracks
    .filter(F.array_contains(F.col("moods"), "chill"))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

joined = (
    recent_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        F.col("a.debut_year").alias("artist_debut_year"),
        "a.artist_name",
    )
)

artist_id_stats = (
    joined
    .groupBy("artist_id")
    .agg(F.avg("artist_debut_year").alias("avg_artist_debut_year"))
)

vs_group = (
    joined.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.artist_debut_year") > F.col("g.avg_artist_debut_year"))
    .select(
        "s.*",
        "g.avg_artist_debut_year",
        F.round(F.col("s.artist_debut_year") - F.col("g.avg_artist_debut_year"), 2).alias("artist_debut_year_minus_avg"),
    )
)

artist_id_stats_2 = (
    vs_group
    .groupBy("artist_id")
    .agg(F.avg("artist_debut_year").alias("avg_artist_debut_year_2"))
)

above_average = (
    vs_group.alias("s")
    .join(artist_id_stats_2.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.artist_debut_year") > F.col("g.avg_artist_debut_year_2"))
    .select(
        "s.*",
        "g.avg_artist_debut_year_2",
        F.round(F.col("s.artist_debut_year") - F.col("g.avg_artist_debut_year_2"), 2).alias("artist_debut_year_minus_avg_2"),
    )
)

result = (
    above_average
    .select("bpm", "artist_id")
    .distinct()
    .orderBy(F.col("bpm").asc(), F.col("artist_id").desc())
)
