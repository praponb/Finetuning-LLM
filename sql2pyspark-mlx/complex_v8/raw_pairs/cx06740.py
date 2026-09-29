from pyspark.sql import functions as F

prepared = (
    df_tracks
    .filter(F.col("title").isNotNull())
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
    )
)

filtered = (
    prepared.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.bpm") >= 96) & (F.col("t.is_explicit")), "left_anti")
)

filtered_2 = (
    filtered.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.ms_played") <= 246779) & (F.col("p.listener_id").isNotNull()), "left_semi")
)

enriched_artist = (
    filtered_2.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.bpm",
        "s.moods",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

result = (
    enriched_artist
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        "artist_genre",
        "artist_is_verified",
        "artist_debut_year",
        F.when(F.col("artist_is_verified"), "yes").otherwise("no").alias("artist_is_verified_label"),
        F.substring(F.trim(F.col("title")), 1, 4).alias("title_prefix"),
    )
    .orderBy(F.col("title").desc())
)
