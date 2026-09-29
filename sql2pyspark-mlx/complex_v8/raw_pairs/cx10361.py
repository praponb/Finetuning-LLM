from pyspark.sql import functions as F

filtered = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "bpm",
        "moods",
        F.date_format(F.col("release_date"), "yyyy-MM").alias("release_date_label"),
        F.length(F.trim(F.col("title"))).alias("title_len"),
        F.upper(F.trim(F.col("title"))).alias("title_clean"),
    )
)

enriched_artist = (
    filtered.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.bpm",
        "s.moods",
        "s.release_date_label",
        "s.title_len",
        "s.title_clean",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.country").alias("artist_country"),
    )
)

result = (
    enriched_artist
    .select(
        "id",
        "title_len",
        "bpm",
        "duration_sec",
    )
    .unpivot(
        ids=["id"],
        values=["title_len", "bpm", "duration_sec"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
