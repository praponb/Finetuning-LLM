from pyspark.sql import functions as F

valid = df_meter_readings.filter(F.col("read_at") >= F.lit("2025-01-31 12:00:00").cast("timestamp"))

reading_rows = (
    valid
    .filter(F.col("site_id") >= 2)
    .select(F.col("site_id").alias("entity_id"), F.col("quality").alias("name"))
)

site_rows = df_sites.select(F.col("id").alias("entity_id"), F.col("zone").alias("name"))

result = reading_rows.union(site_rows)
