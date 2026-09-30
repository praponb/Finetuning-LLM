from pyspark.sql import functions as F

site_side = df_sites.select("id")

reading_rows = df_meter_readings.select(F.col("site_id").alias("id"))

result = (
    site_side
    .intersectAll(reading_rows)
    .orderBy(F.col("id").desc())
)
