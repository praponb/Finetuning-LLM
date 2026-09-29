from pyspark.sql import functions as F

filtered_readings = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) & (F.col("kwh") < 106.0))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.when(F.col("voltage") >= 232.7, "large").when(F.col("voltage") >= 224.8, "medium").otherwise("small").alias("voltage_tier"),
        F.when(F.col("quality").isin("EST", "BAD"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
    )
)

with_details_site = (
    filtered_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality_slug",
        "s.quality_group",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.zone").alias("site_zone"),
    )
)

quality_group_stats = (
    with_details_site
    .groupBy("quality_group")
    .agg(F.percentile_approx("voltage", 0.5).alias("median_voltage"))
)

above_average = (
    with_details_site.alias("s")
    .join(quality_group_stats.alias("g"), F.col("g.quality_group") == F.col("s.quality_group"), "inner")
    .filter(F.col("s.voltage") >= F.col("g.median_voltage"))
    .select(
        "s.*",
        "g.median_voltage",
        F.round(F.col("s.voltage") - F.col("g.median_voltage"), 2).alias("voltage_minus_median"),
    )
)

summary = (
    above_average
    .filter(F.col("quality_group").isin("group_a", "other"))
    .groupBy(
        F.date_trunc("month", "read_at").alias("read_at_month"),
        "site_id",
        "site_zone",
    )
    .agg(
        F.min("voltage_minus_median").alias("min_voltage_minus_median"),
        F.count("*").alias("row_count"),
    )
)

result = (
    summary
    .groupBy("row_count", "read_at_month")
    .pivot("site_zone", ["Z2", "Z3"])
    .agg(F.max("min_voltage_minus_median"))
)
