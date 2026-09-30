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
    )
)

quality_stats = (
    valid_readings
    .groupBy("quality")
    .agg(F.percentile_approx("voltage", 0.5).alias("median_voltage"))
)

above_average = (
    valid_readings.alias("s")
    .join(quality_stats.alias("g"), F.col("g.quality") == F.col("s.quality"), "inner")
    .filter(F.col("s.voltage") >= F.col("g.median_voltage"))
    .select(
        "s.*",
        "g.median_voltage",
        F.round(F.col("s.voltage") - F.col("g.median_voltage"), 2).alias("voltage_minus_median"),
    )
)

totals = (
    above_average
    .filter(F.col("site_id").isNotNull())
    .groupBy("site_id", "quality")
    .agg(
        F.sum(F.when(F.col("quality").isin("BAD", "EST"), F.col("kwh")).otherwise(0)).alias("kwh_quality_in_bad_est"),
        F.min("voltage_minus_median").alias("min_voltage_minus_median"),
        F.countDistinct("quality").alias("distinct_qualitys"),
        F.avg("voltage_minus_median").alias("avg_voltage_minus_median_check"),
    )
    .filter(F.col("avg_voltage_minus_median_check") > -49.0)
    .drop("avg_voltage_minus_median_check")
)

screened = (
    totals.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality") == "BAD"), "left_semi")
)

enriched = (
    screened.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (~F.col("si.zone").isin("Z3", "Z2")), "left")
    .select(
        "s.*",
        "si.site_name",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

result = (
    enriched
    .groupingSets(
        [["quality", "site_id"], ["quality"]],
        "quality", "site_id",
    )
    .agg(
        F.grouping("quality").alias("is_total_quality"),
        F.round(F.sum("kwh_quality_in_bad_est"), 2).alias("total_kwh_quality_in_bad_est"),
    )
)
