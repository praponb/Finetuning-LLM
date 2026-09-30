from pyspark.sql import functions as F

site_side = df_sites.select(F.col("id").alias("entity_id"), F.col("tariff").alias("label"))

reading_rows = df_meter_readings.select("site_id", "quality")

result = (
    site_side
    .intersectAll(reading_rows)
    .orderBy("label", "entity_id")
)
