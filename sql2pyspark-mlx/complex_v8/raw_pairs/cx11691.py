from pyspark.sql import functions as F

valid_tracks = (
    df_tracks
    .filter(F.col("release_date").isNotNull())
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
    )
)

enriched_artist = (
    valid_tracks.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.length(F.trim(F.col("a.artist_name"))) >= 5), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        F.col("a.genre").alias("artist_genre"),
    )
)

scored = (
    enriched_artist
    .select(
        "id",
        "artist_id",
        "title",
        "artist_genre",
        F.concat(F.col("artist_genre"), F.lit("#"), F.col("id").cast("string")).alias("artist_genre_ref"),
        F.concat(F.col("title"), F.lit("#"), F.col("id").cast("string")).alias("title_ref"),
        F.when(F.col("artist_genre").isin("Indie", "Jazz"), "group_a").when(F.col("artist_genre") == "Pop", "group_b").otherwise("other").alias("artist_genre_group"),
    )
)

result = (
    scored
    .filter(F.col("title").isNotNull())
    .select(
        "id",
        "artist_id",
        "title",
        "artist_genre",
        "artist_genre_group",
        F.initcap(F.trim(F.col("title_ref"))).alias("title_ref_clean"),
        F.lower(F.regexp_replace(F.trim(F.col("artist_genre")), " ", "_")).alias("artist_genre_slug"),
    )
    .orderBy(F.col("title_ref_clean").desc(), F.col("title").desc())
)
