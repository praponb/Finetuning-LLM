from pyspark.sql import functions as F

recent_readings = (
    df_meter_readings
    .filter((F.col("kwh") <= 113.0) & (F.col("quality").isin("BAD", "OK")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_per_voltage"),
    )
)

enriched = (
    recent_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.zone").isin("Z4", "Z2"))
    .select("s.*", F.col("si.zone").alias("site_zone"))
)

per_group = (
    enriched
    .groupBy("quality")
    .agg(
        F.round(F.sum("kwh_per_voltage") / F.nullif(F.sum("voltage"), F.lit(0)), 3).alias("kwh_per_voltage_to_voltage_ratio"),
        F.min("kwh_per_voltage").alias("min_kwh_per_voltage"),
        F.round(F.sum("kwh"), 2).alias("total_kwh"),
        F.countDistinct("site_zone").alias("distinct_site_zones"),
        F.sum("kwh").alias("sum_kwh_check"),
    )
    .filter(F.col("sum_kwh_check") > 35.0)
    .drop("sum_kwh_check")
)

without_matches = (
    per_group
    .crossJoin(per_group.agg(F.max("min_kwh_per_voltage").alias("overall_max_min_kwh_per_voltage")))
    .filter(F.col("min_kwh_per_voltage") < F.col("overall_max_min_kwh_per_voltage"))
    .drop("overall_max_min_kwh_per_voltage")
)

result = (
    without_matches
    .select(
        "quality",
        "min_kwh_per_voltage",
        "total_kwh",
        "kwh_per_voltage_to_voltage_ratio",
    )
    .unpivot(
        ids=["quality"],
        values=["min_kwh_per_voltage", "total_kwh", "kwh_per_voltage_to_voltage_ratio"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
)
