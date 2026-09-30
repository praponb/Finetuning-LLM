from pyspark.sql import functions as F

valid = (
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

reading_rows = valid.select("id", F.col("site_id").alias("qty"))

reading_side = (
    df_meter_readings
    .filter((F.col("kwh") >= 26.9) & (F.col("site_id") <= 6))
    .select(F.col("site_id").alias("id"), F.col("id").alias("qty"))
)

result = (
    reading_rows
    .unionByName(reading_side)
    .orderBy(F.col("id").desc())
)
