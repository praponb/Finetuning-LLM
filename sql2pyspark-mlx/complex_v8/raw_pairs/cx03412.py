from pyspark.sql import functions as F

reading_side = (
    df_meter_readings
    .filter((F.col("site_id") <= 8) & (F.col("read_at") >= F.lit("2025-01-13 13:00:00").cast("timestamp")))
    .select(
        F.col("id").alias("key_id"),
        F.col("site_id").alias("entity_id"),
        F.col("kwh").alias("metric"),
    )
)

reading_side_2 = df_meter_readings.select("id", "site_id", "kwh")

reading_side_3 = df_meter_readings.select("id", "site_id", "voltage")

stacked = (
    reading_side
    .union(reading_side_2)
    .union(reading_side_3)
)

result = (
    stacked
    .groupBy("key_id")
    .agg(F.count("*").alias("n_rows"), F.sum("metric").alias("total_metric"))
    .orderBy("key_id")
)
