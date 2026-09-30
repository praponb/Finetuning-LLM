from pyspark.sql import functions as F

cleaned_tracks = (
    df_tracks
    .filter((F.col("artist_id").isNotNull()) & (~F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
    )
)

grouped = (
    cleaned_tracks
    .groupBy(
        "artist_id",
        F.date_trunc("month", "release_date").alias("release_date_month"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("artist_id").alias("distinct_artist_ids"),
    )
)

joined_artist = (
    grouped.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.artist_id",
        "s.release_date_month",
        "s.row_count",
        "s.distinct_artist_ids",
        "a.artist_name",
    )
)

result = (
    joined_artist
    .select(
        "artist_id",
        "release_date_month",
        "row_count",
        "distinct_artist_ids",
        "artist_name",
    )
    .orderBy(F.col("release_date_month").desc(), "distinct_artist_ids")
)
