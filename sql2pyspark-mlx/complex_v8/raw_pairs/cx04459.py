from pyspark.sql import functions as F

base = (
    df_tracks
    .filter((F.col("artist_id").isNotNull()) | (F.col("title").isNotNull()))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.length(F.trim(F.col("title"))).alias("title_len"),
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
        F.date_trunc("quarter", F.col("release_date")).alias("release_date_quarter"),
    )
)

enriched = (
    base.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.bpm",
        "s.title_len",
        "s.is_explicit_flag",
        "s.release_date_quarter",
        F.col("a.country").alias("artist_country"),
    )
)

with_details_artist = (
    enriched.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select("s.*", F.col("a.genre").alias("artist_genre"))
)

result = (
    with_details_artist
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        "title_len",
        "is_explicit_flag",
        "release_date_quarter",
        "artist_country",
        F.abs(F.col("title_len") - 6).alias("title_len_distance"),
        F.concat_ws("|", F.col("artist_genre"), F.col("title")).alias("artist_genre_title_label"),
    )
    .orderBy(F.col("title_len_distance").desc_nulls_first(), "id")
    .limit(5)
)
