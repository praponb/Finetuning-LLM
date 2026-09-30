from pyspark.sql import functions as F

filtered = (
    df_meter_readings
    .filter((F.col("voltage") <= 231.7) & (F.col("kwh") >= 34.6))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.coalesce(F.col("voltage"), F.lit(0.0)).alias("voltage_filled"),
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

line_level_readings = (
    filtered.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.days_since_read_at",
        F.col("m.id").alias("reading_id"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

grouped = (
    line_level_readings
    .groupBy(
        "site_id",
        F.when(F.col("reading_voltage") >= 238.8, "A").when(F.col("reading_voltage") >= 219.5, "B").otherwise("C").alias("reading_voltage_tier"),
        "reading_id",
    )
    .agg(
        F.percentile_approx("reading_voltage", 0.5).alias("median_reading_voltage"),
        F.round(F.stddev("reading_kwh"), 2).alias("stddev_reading_kwh"),
        F.min("reading_voltage").alias("min_reading_voltage"),
    )
)

qualified = (
    grouped.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality") == "OK"), "left_anti")
)

result = (
    qualified
    .select(
        "site_id",
        "reading_voltage_tier",
        F.col("median_reading_voltage").alias("median_reading_voltage_v"),
        F.col("min_reading_voltage").alias("min_reading_voltage_v"),
    )
    .unpivot(
        ids=["site_id", "reading_voltage_tier"],
        values=["median_reading_voltage_v", "min_reading_voltage_v"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
