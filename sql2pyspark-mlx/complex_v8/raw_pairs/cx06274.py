from pyspark.sql import functions as F

scoped = (
    df_meter_readings
    .filter((F.col("voltage") > 216.5) & (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count(F.when(F.col("quality") == "BAD", 1)).alias("reading_quality_count"),
    )
)

with_counts = (
    scoped.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
    )
)

result = (
    with_counts
    .rollup("reading_quality_count", "site_id", "quality")
    .agg(
        F.max("kwh_filled").alias("max_kwh_filled"),
        F.round(F.sum("reading_quality_count") / F.nullif(F.sum("kwh_filled"), F.lit(0)), 3).alias("reading_quality_count_to_kwh_filled_ratio"),
    )
    .orderBy(
        F.col("reading_quality_count").asc_nulls_last(),
        F.col("site_id").asc_nulls_last(),
        F.col("quality").asc_nulls_last(),
    )
)
