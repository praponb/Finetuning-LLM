from pyspark.sql import functions as F

scoped_plays = (
    df_plays
    .filter((F.col("ms_played").between(26344, 290754)) & (F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        F.upper(F.trim(F.col("source"))).alias("source_clean"),
    )
)

result = (
    scoped_plays
    .filter(F.col("listener_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "is_skipped",
        "source",
        F.substring(F.trim(F.col("source")), 1, 2).alias("source_prefix"),
        F.length(F.trim(F.col("source"))).alias("source_len"),
    )
    .orderBy(F.col("id").desc(), F.col("source").desc())
)
