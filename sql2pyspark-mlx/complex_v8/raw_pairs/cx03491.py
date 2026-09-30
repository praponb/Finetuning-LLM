from pyspark.sql import functions as F

reading_rows = (
    df_meter_readings
    .filter(F.col("voltage") >= 218.5)
    .select("id", F.col("read_at").alias("happened_at"), "kwh")
)

reading_rows_2 = (
    df_meter_readings
    .filter((F.col("quality") == "OK") & (F.col("site_id").isNotNull()))
    .select("id", "read_at", "kwh")
)

reading_side = df_meter_readings.select("site_id", "read_at", "voltage")

result = (
    reading_rows
    .union(reading_rows_2)
    .union(reading_side)
    .orderBy(F.col("happened_at").desc())
)
