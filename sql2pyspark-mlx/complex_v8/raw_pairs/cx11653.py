from pyspark.sql import functions as F

filtered_readings = (
    df_meter_readings
    .filter(F.col("voltage") >= 224.8)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
    )
)

detail_rows_readings = (
    filtered_readings.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        F.col("m.id").alias("reading_id"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

result = (
    detail_rows_readings
    .select(
        "id",
        "reading_id",
        "reading_kwh",
        "kwh",
    )
    .unpivot(
        ids=["id", "reading_id"],
        values=["reading_kwh", "kwh"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "reading_id", "field")
)
