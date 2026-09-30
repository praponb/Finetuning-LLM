from pyspark.sql import functions as F

filtered = (
    df_tracks
    .filter(F.col("bpm") <= 166)
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "moods",
    )
)

result = (
    filtered
    .filter(~F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "moods",
        F.concat(F.col("title"), F.lit("#"), F.col("id").cast("string")).alias("title_ref"),
        F.array_contains(F.col("moods"), "focus").alias("has_focus"),
    )
    .orderBy(F.col("title_ref").desc(), F.col("artist_id").desc_nulls_first(), "id")
    .limit(5)
)
