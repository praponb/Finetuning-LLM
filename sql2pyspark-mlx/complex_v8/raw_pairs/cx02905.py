from pyspark.sql import functions as F

cleaned = (
    df_plays
    .filter((F.col("listener_id").isNotNull()) & (F.col("device").isin("phone", "speaker")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

derived = (
    cleaned
    .select(
        "*",
        F.abs(F.col("days_since_played_at") - 350).alias("days_since_played_at_distance"),
        F.substring(F.trim(F.col("source")), 1, 3).alias("source_prefix"),
    )
)

result = (
    derived
    .groupingSets(
        [["source", "listener_id"], ["source"], ["listener_id"]],
        "source", "listener_id",
    )
    .agg(
        F.sum("days_since_played_at").alias("total_days_since_played_at"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .select(
        F.coalesce(F.col("source"), F.lit("ALL")).alias("source"),
        "listener_id",
        "total_days_since_played_at",
        "distinct_ids",
    )
    .orderBy(F.col("source").asc_nulls_last(), F.col("listener_id").asc_nulls_last())
)
