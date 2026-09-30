from pyspark.sql import functions as F

scoped_readings = (
    df_meter_readings
    .filter(F.col("voltage") <= 233.6)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

reading_rows = (
    scoped_readings
    .filter(F.col("quality").isin("BAD", "EST"))
    .select("id", F.col("site_id").alias("key_id"))
)

reading_side = df_meter_readings.select("id", "site_id")

result = (
    reading_rows
    .union(reading_side)
    .distinct()
    .orderBy("key_id")
)
