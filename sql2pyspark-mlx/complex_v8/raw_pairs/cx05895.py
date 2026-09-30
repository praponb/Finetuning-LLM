from pyspark.sql import functions as F

base_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
    )
)

result = (
    base_plays
    .select(
        "id",
        "track_id",
        "is_skipped",
        F.upper(F.trim(F.col("source"))).alias("source_clean"),
    )
    .orderBy(F.col("source_clean").desc(), F.col("id").asc_nulls_last())
)
