from pyspark.sql import functions as F

cleaned_readings = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
    )
)

with_activity = (
    cleaned_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.read_at") < F.lit("2025-02-12 14:00:00").cast("timestamp")) & (F.col("m.quality").isin("EST", "OK")), "left_anti")
)

result = (
    with_activity
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
    )
    .orderBy("voltage", "id")
    .limit(10)
)
