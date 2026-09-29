from pyspark.sql import functions as F

filtered_tracks = (
    df_tracks
    .filter(F.col("duration_sec") >= 183)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

artist_id_stats = (
    filtered_tracks
    .groupBy("artist_id")
    .agg(F.avg("bpm").alias("avg_bpm"))
)

vs_group = (
    filtered_tracks.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.bpm") > F.col("g.avg_bpm"))
    .select(
        "s.*",
        "g.avg_bpm",
        F.round(F.col("s.bpm") - F.col("g.avg_bpm"), 2).alias("bpm_minus_avg"),
    )
)

with_details = (
    vs_group.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.country").isin("US", "BR", "TH")), "inner")
    .filter(F.col("a.genre") == "Pop")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.bpm_minus_avg",
        F.col("a.genre").alias("artist_genre"),
    )
)

artist_genre_stats = (
    with_details
    .groupBy("artist_genre")
    .agg(F.avg("bpm_minus_avg").alias("avg_bpm_minus_avg"))
)

above_average = (
    with_details.alias("s")
    .join(artist_genre_stats.alias("g"), F.col("g.artist_genre") == F.col("s.artist_genre"), "inner")
    .filter(F.col("s.bpm_minus_avg") > F.col("g.avg_bpm_minus_avg"))
    .select(
        "s.*",
        "g.avg_bpm_minus_avg",
        F.round(F.col("s.bpm_minus_avg") - F.col("g.avg_bpm_minus_avg"), 2).alias("bpm_minus_avg_minus_avg"),
    )
)

result = (
    above_average
    .groupingSets(
        [["artist_genre", "artist_id"], ["artist_genre"]],
        "artist_genre", "artist_id",
    )
    .agg(F.count("*").alias("row_count"))
    .select(
        F.coalesce(F.col("artist_genre"), F.lit("ALL")).alias("artist_genre"),
        "artist_id",
        "row_count",
    )
)
