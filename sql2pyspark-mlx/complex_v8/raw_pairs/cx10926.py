from pyspark.sql import functions as F

base = (
    df_plays
    .filter((~F.col("is_skipped")) & (F.to_date(F.col("played_at")) < F.lit("2025-02-24").cast("date")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        F.abs(F.col("ms_played") - 176302).alias("ms_played_distance"),
        F.month(F.col("played_at")).alias("played_at_month"),
        F.lower(F.trim(F.col("source"))).alias("source_clean"),
    )
)

derived = (
    base
    .filter(F.col("track_id") <= 24)
    .select(
        "*",
        (F.col("ms_played_distance") + F.col("ms_played")).alias("ms_played_distance_plus_ms_played"),
        F.quarter(F.col("played_at")).alias("played_at_quarter"),
        F.when(F.col("device").isin("desktop", "car"), "group_a").when(F.col("device") == "tv", "group_b").otherwise("other").alias("device_group"),
    )
)

rollup_base = (
    derived
    .filter(F.col("ms_played_distance_plus_ms_played") <= 720000)
    .groupBy(
        F.when(F.col("ms_played") >= 220498, "large").when(F.col("ms_played") >= 98052, "medium").otherwise("small").alias("ms_played_tier"),
        "device_group",
    )
    .agg(
        F.sum("ms_played_distance_plus_ms_played").alias("total_ms_played_distance_plus_ms_played"),
        F.countDistinct("listener_id").alias("distinct_listener_ids"),
        F.sum(F.when(F.col("device") == "speaker", 1).otherwise(0)).alias("n_device_speaker"),
    )
)

filtered = (
    rollup_base
    .crossJoin(rollup_base.agg(F.avg("total_ms_played_distance_plus_ms_played").alias("overall_avg_total_ms_played_distance_plus_ms_pla")))
    .filter(F.col("total_ms_played_distance_plus_ms_played") > F.col("overall_avg_total_ms_played_distance_plus_ms_pla"))
    .drop("overall_avg_total_ms_played_distance_plus_ms_pla")
)

result = (
    filtered
    .filter(F.col("total_ms_played_distance_plus_ms_played").between(1680000, 4560000))
    .select(
        "ms_played_tier",
        "device_group",
        "total_ms_played_distance_plus_ms_played",
        "distinct_listener_ids",
        (F.col("distinct_listener_ids") - F.col("n_device_speaker")).alias("distinct_listener_ids_minus_n_device_speaker"),
    )
    .orderBy(F.col("total_ms_played_distance_plus_ms_played").asc())
)
