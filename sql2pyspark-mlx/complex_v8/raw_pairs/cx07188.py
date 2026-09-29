from pyspark.sql import functions as F

reading_side = (
    df_meter_readings
    .select(F.col("id").alias("qty"), "voltage", F.col("site_id").alias("key_id"))
)

reading_rows = (
    df_meter_readings
    .filter(F.month(F.col("read_at")).isin(2, 3))
    .select("site_id", "kwh", "id")
)

result = (
    reading_side
    .union(reading_rows)
    .orderBy(F.col("key_id").desc())
)
