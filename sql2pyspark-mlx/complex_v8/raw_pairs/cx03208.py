from pyspark.sql import functions as F

filtered = (
    df_plays
    .filter(F.col("source") == "radio")
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
    )
)

tagged = (
    filtered
    .select(
        "*",
        F.lower(F.trim(F.col("source"))).alias("source_clean"),
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
    )
)

result = (
    tagged
    .filter(F.col("listener_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "device",
        "is_skipped",
        "source",
        "source_clean",
        F.length(F.trim(F.col("device_or_unknown"))).alias("device_or_unknown_len"),
        F.length(F.trim(F.col("source_clean"))).alias("source_clean_len"),
    )
    .orderBy(F.col("device").desc_nulls_first(), F.col("source_clean").asc(), "id")
    .limit(5)
)
