from pyspark.sql import functions as F

site_rows = df_sites.select(F.col("id").alias("key_id"), "tariff")

reading_rows = (
    df_meter_readings
    .select(F.col("site_id").alias("key_id"), F.col("quality").alias("tariff"))
)

result = site_rows.unionByName(reading_rows)
