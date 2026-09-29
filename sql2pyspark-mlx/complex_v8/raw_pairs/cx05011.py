from pyspark.sql import functions as F

scoped = (
    df_tracks
    .filter(F.col("duration_sec") > 154)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        F.lower(F.trim(F.col("title"))).alias("title_clean"),
        F.length(F.trim(F.col("title"))).alias("title_len"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("release_date")).alias("days_since_release_date"),
    )
)

enriched_artist = (
    scoped.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.length(F.trim(F.col("a.artist_name"))) >= 5)
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.title_clean",
        "s.title_len",
        "a.artist_name",
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.genre").alias("artist_genre"),
    )
)

with_parent = (
    enriched_artist.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.*",
        F.col("a.debut_year").alias("artist_debut_year_2"),
        F.col("a.artist_name").alias("artist_artist_name"),
        F.col("a.genre").alias("artist_genre_2"),
    )
)

result = (
    with_parent
    .groupingSets(
        [["artist_genre", "artist_genre_2"], ["artist_genre"], []],
        "artist_genre", "artist_genre_2",
    )
    .agg(
        F.count(F.when(F.lower(F.col("artist_artist_name")).like("%er%"), 1)).alias("n_lower_artist_artist_name_lik"),
        F.percentile_approx("title_len", 0.5).alias("median_title_len"),
        F.count("*").alias("row_count"),
    )
)
