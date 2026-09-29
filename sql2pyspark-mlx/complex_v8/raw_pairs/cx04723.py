from pyspark.sql import functions as F

recent = (
    df_meter_readings
    .filter(F.hour(F.col("read_at")).between(8, 12))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.substring(F.trim(F.col("quality")), 1, 3).alias("quality_prefix"),
    )
)

enriched = (
    recent
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        "quality_prefix",
        (F.col("voltage") - F.col("kwh")).alias("voltage_minus_kwh"),
    )
)

screened = (
    enriched.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality").isNotNull()) & (F.col("m.voltage") < 229.1), "left_anti")
    .filter(F.col("quality").isin("BAD", "EST"))
)

labeled = (
    screened
    .filter(F.col("quality") == "BAD")
    .select(
        "id",
        "site_id",
        F.round(F.col("voltage") * 1.07, 2).alias("voltage_adj"),
        F.when(F.col("voltage") >= 236.7, "A").when(F.col("voltage") >= 222.3, "B").otherwise("C").alias("voltage_tier"),
    )
)

result = (
    labeled
    .rollup("site_id", "voltage_tier")
    .agg(
        F.grouping("site_id").alias("is_total_site_id"),
        F.countDistinct("site_id").alias("distinct_site_ids"),
        F.count("*").alias("cnt"),
    )
    .orderBy(
        F.col("site_id").asc_nulls_last(),
        F.col("voltage_tier").asc_nulls_last(),
    )
)
