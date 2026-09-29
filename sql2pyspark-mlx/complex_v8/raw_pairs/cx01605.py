from pyspark.sql import functions as F

reading_side = (
    df_meter_readings
    .filter((F.col("quality").isin("EST", "OK")) & (F.col("read_at") >= F.lit("2025-01-31 12:00:00").cast("timestamp")))
    .select(F.col("site_id").alias("key_id"))
)

site_side = df_sites.select("id")

result = (
    reading_side
    .subtract(site_side)
    .orderBy("key_id")
)
