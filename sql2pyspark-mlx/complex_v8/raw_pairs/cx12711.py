from pyspark.sql import functions as F

site_side = (
    df_sites
    .filter((F.upper(F.trim(F.col("site_name"))).like("R%")) & (F.col("zone") == "Z2"))
    .select(F.col("id").alias("key_id"), "tariff")
)

reading_side = df_meter_readings.select("id", "quality")

result = site_side.intersect(reading_side)
