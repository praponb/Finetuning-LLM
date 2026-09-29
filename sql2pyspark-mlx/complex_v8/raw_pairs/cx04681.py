from pyspark.sql import functions as F

prepared = (
    df_tracks
    .filter((F.col("release_date").between(F.lit("2024-10-23").cast("date"), F.lit("2025-11-29").cast("date"))) & (F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "moods",
    )
)

result = (
    prepared
    .select(
        "id",
        "title",
        "is_explicit",
        "moods",
        F.substring(F.trim(F.col("title")), 1, 4).alias("title_prefix"),
    )
    .orderBy(F.col("title").asc_nulls_last())
)
