from pyspark.sql import functions as F

base_tracks = (
    df_tracks
    .filter((F.col("release_date").isNotNull()) & (F.col("duration_sec") >= 184))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        "moods",
        F.date_trunc("quarter", F.col("release_date")).alias("release_date_quarter"),
        F.substring(F.trim(F.col("title")), 1, 4).alias("title_prefix"),
        F.substring(F.trim(F.col("title")), 1, 2).alias("title_prefix_2"),
    )
)

per_label = (
    base_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        "release_date_quarter",
        "title_prefix",
        "title_prefix_2",
        F.explode("moods").alias("mood"),
    )
)

joined = (
    per_label.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.genre").isin("Jazz", "Pop"))
    .select("s.*", F.col("a.genre").alias("artist_genre"))
)

result = (
    joined
    .filter(F.lower(F.col("title")).like("%in%"))
    .select(
        "id",
        "artist_id",
        "title",
        "bpm",
        "release_date_quarter",
        "title_prefix",
        "title_prefix_2",
        "artist_genre",
        F.substring(F.trim(F.col("artist_genre")), 1, 2).alias("artist_genre_prefix"),
    )
    .orderBy(F.col("title_prefix_2").desc())
)
