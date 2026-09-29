from pyspark.sql import functions as F

prepared_readings = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) & (F.col("voltage") >= 215.6))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

scored = (
    prepared_readings
    .select(
        "id",
        "site_id",
        "voltage",
        "quality",
        "quality_len",
        F.round(F.col("kwh") * 1.1, 2).alias("kwh_adj"),
    )
)

result = (
    scored
    .cube("site_id", "quality_len")
    .agg(
        F.countDistinct("site_id").alias("distinct_site_ids"),
        F.count("*").alias("cnt"),
        F.round(100 * F.count(F.when(F.col("quality") == "EST", 1)) / F.count("*"), 1).alias("pct_quality_est"),
    )
    .orderBy(
        F.col("site_id").asc_nulls_last(),
        F.col("quality_len").asc_nulls_last(),
    )
)
