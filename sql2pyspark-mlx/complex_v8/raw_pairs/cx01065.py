from pyspark.sql import functions as F

cleaned_tracks = (
    df_tracks
    .filter((~F.array_contains(F.col("moods"), "focus")) & (F.col("bpm") >= 89))
    .select(
        "id",
        "artist_id",
        "title",
        "bpm",
        "moods",
    )
)

enriched = (
    cleaned_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.bpm",
        "s.moods",
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.max("played_at").alias("last_played_at"),
        F.max("played_at").alias("last_played_at_2"),
    )
)

summarized_rows = (
    enriched.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select("s.*", "p.last_played_at", "p.last_played_at_2")
)

exploded = (
    summarized_rows
    .select(
        "id",
        "artist_id",
        "title",
        "bpm",
        "artist_debut_year",
        "artist_is_verified",
        "last_played_at",
        "last_played_at_2",
        F.explode("moods").alias("mood"),
    )
)

with_details = (
    exploded.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.bpm",
        "s.artist_debut_year",
        "s.artist_is_verified",
        "s.last_played_at_2",
        "s.mood",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.country").alias("artist_country"),
    )
)

result = (
    with_details
    .select(
        "id",
        "artist_id",
        "mood",
        "artist_genre",
        "artist_country",
    )
    .orderBy(
        F.col("artist_country").asc_nulls_last(),
        F.col("artist_genre").desc_nulls_first(),
    )
)
