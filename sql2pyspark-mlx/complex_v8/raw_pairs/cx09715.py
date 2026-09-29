from pyspark.sql import functions as F

prepared_readings = (
    df_meter_readings
    .filter(F.month(F.col("read_at")).isin(2, 3))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
    )
)

reading_side = prepared_readings.select("id")

reading_rows = df_meter_readings.select(F.col("site_id").alias("id"))

result = (
    reading_side
    .subtract(reading_rows)
    .orderBy("id")
)
