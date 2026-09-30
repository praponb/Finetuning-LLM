from pyspark.sql import functions as F

base_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
    )
)

result = (
    base_readings
    .rollup("quality", "site_id")
    .agg(
        F.count("*").alias("cnt"),
        F.round(100 * F.count(F.when(F.col("quality").isNotNull(), 1)) / F.count("*"), 1).alias("pct_quality_is_not_null"),
        F.countDistinct("site_id").alias("distinct_site_ids"),
    )
)
