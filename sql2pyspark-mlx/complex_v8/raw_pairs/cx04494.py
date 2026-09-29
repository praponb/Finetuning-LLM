from pyspark.sql import functions as F

valid_readings = (
    df_meter_readings
    .filter(F.col("quality") == "BAD")
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.upper(F.trim(F.col("quality"))).alias("quality_clean"),
        F.round(F.col("voltage") * 1.5, 2).alias("voltage_adj"),
        F.when(F.col("kwh") >= 89.8, "A").when(F.col("kwh") >= 51.5, "B").otherwise("C").alias("kwh_tier"),
    )
)

kwh_tier_stats = (
    valid_readings
    .groupBy("kwh_tier")
    .agg(F.avg("voltage").alias("avg_voltage"))
)

compared = (
    valid_readings.alias("s")
    .join(kwh_tier_stats.alias("g"), F.col("g.kwh_tier") == F.col("s.kwh_tier"), "inner")
    .filter(F.col("s.voltage") > F.col("g.avg_voltage"))
    .select(
        "s.*",
        "g.avg_voltage",
        F.round(F.col("s.voltage") - F.col("g.avg_voltage"), 2).alias("voltage_minus_avg"),
    )
)

result = (
    compared
    .select(
        "id",
        "voltage_adj",
        "avg_voltage",
        "voltage",
        "voltage_minus_avg",
    )
    .unpivot(
        ids=["id"],
        values=["voltage_adj", "avg_voltage", "voltage", "voltage_minus_avg"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
