from pyspark.sql import functions as F

prepared_tracks = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "moods",
        F.date_trunc("year", F.col("release_date")).alias("release_date_year"),
        F.date_add(F.col("release_date"), 30).alias("release_date_plus_30d"),
    )
)

enriched = (
    prepared_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.moods",
        "s.release_date_plus_30d",
        F.col("a.country").alias("artist_country"),
        "a.artist_name",
        F.col("a.genre").alias("artist_genre"),
    )
)

exploded = (
    enriched
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date_plus_30d",
        "artist_country",
        "artist_name",
        "artist_genre",
        F.posexplode("moods").alias("mood_pos", "mood"),
    )
)

result = (
    exploded
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date_plus_30d",
        "artist_country",
        "artist_name",
        "artist_genre",
        "mood",
    )
    .orderBy(F.col("title").desc_nulls_first())
)
