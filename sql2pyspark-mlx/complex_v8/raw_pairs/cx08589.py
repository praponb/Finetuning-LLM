from pyspark.sql import functions as F

recent_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
    )
)

rollup_base = (
    recent_readings
    .groupBy(F.year("read_at").alias("read_at_year"))
    .agg(
        F.round(100 * F.count(F.when(F.col("quality") == "EST", 1)) / F.count("*"), 1).alias("pct_quality_est"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
    )
)

screened = (
    rollup_base
    .crossJoin(rollup_base.agg(F.avg("pct_quality_est").alias("overall_avg_pct_quality_est")))
    .filter(F.col("pct_quality_est") > F.col("overall_avg_pct_quality_est"))
    .drop("overall_avg_pct_quality_est")
)

result = (
    screened
    .filter(F.col("pct_quality_est") >= 40.0)
    .select(
        "read_at_year",
        "pct_quality_est",
        F.coalesce(F.col("pct_quality_est"), F.lit(0.0)).alias("pct_quality_est_filled"),
        F.coalesce(F.col("pct_quality_est"), F.lit(0.0)).alias("pct_quality_est_filled_2"),
    )
    .orderBy(
        F.col("pct_quality_est").asc(),
        F.col("pct_quality_est_filled").desc_nulls_first(),
    )
)
