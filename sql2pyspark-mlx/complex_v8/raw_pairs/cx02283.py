from pyspark.sql import functions as F

scoped = (
    df_meter_readings
    .filter((F.col("kwh") <= 86.0) & (F.col("read_at") >= F.lit("2025-01-25 13:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

detail_rows_readings = (
    scoped.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.quality",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.voltage").alias("reading_voltage"),
    )
)

result = (
    detail_rows_readings
    .select(
        "id",
        "reading_id",
        "reading_kwh",
        "voltage",
        "kwh",
        "reading_voltage",
    )
    .unpivot(
        ids=["id", "reading_id"],
        values=["reading_kwh", "voltage", "kwh", "reading_voltage"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
