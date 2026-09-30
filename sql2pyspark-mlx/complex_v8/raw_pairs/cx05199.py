from pyspark.sql import functions as F

prepared_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("kwh") >= 101.2, "A").when(F.col("kwh") >= 45.3, "B").otherwise("C").alias("kwh_tier"),
        F.date_format(F.col("read_at"), "yyyy-MM-dd HH:00").alias("read_at_label"),
    )
)

quality_stats = (
    prepared_readings
    .groupBy("quality")
    .agg(F.max("voltage").alias("max_voltage"))
)

outliers = (
    prepared_readings.alias("s")
    .join(quality_stats.alias("g"), F.col("g.quality") == F.col("s.quality"), "inner")
    .filter(F.col("s.voltage") == F.col("g.max_voltage"))
    .select(
        "s.*",
        "g.max_voltage",
        F.round(F.col("s.voltage") - F.col("g.max_voltage"), 2).alias("voltage_minus_max"),
    )
)

result = (
    outliers
    .select(
        "id",
        "voltage_minus_max",
        "voltage",
        "kwh",
        "max_voltage",
    )
    .unpivot(
        ids=["id"],
        values=["voltage_minus_max", "voltage", "kwh", "max_voltage"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
