from pyspark.sql import functions as F

valid_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.greatest(F.col("voltage"), F.col("kwh")).alias("greatest_voltage_kwh"),
        F.when(F.col("voltage") >= 233.8, "A").when(F.col("voltage") >= 224.8, "B").otherwise("C").alias("voltage_tier"),
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref"),
    )
)

with_details_site = (
    valid_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.*",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

result = (
    with_details_site
    .cube("quality", "voltage_tier", "site_id")
    .agg(
        F.round(F.sum("kwh") / F.nullif(F.sum("voltage"), F.lit(0)), 3).alias("kwh_to_voltage_ratio"),
        F.percentile_approx("kwh", 0.5).alias("median_kwh"),
        F.countDistinct("site_tariff").alias("distinct_site_tariffs"),
    )
    .select(
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        F.coalesce(F.col("voltage_tier"), F.lit("ALL")).alias("voltage_tier"),
        "site_id",
        "kwh_to_voltage_ratio",
        "median_kwh",
        "distinct_site_tariffs",
    )
    .orderBy(
        F.col("quality").asc_nulls_last(),
        F.col("voltage_tier").asc_nulls_last(),
        F.col("site_id").asc_nulls_last(),
    )
)
