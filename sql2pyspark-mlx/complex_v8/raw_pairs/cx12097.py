from pyspark.sql import functions as F

site_side = (
    df_sites
    .filter((F.upper(F.trim(F.col("site_name"))).like("R%")) & (F.col("tariff") != "TOU"))
    .select(F.col("id").alias("entity_id"))
)

reading_side = (
    df_meter_readings
    .filter(F.col("site_id") >= 6)
    .select("id")
)

result = (
    site_side
    .intersect(reading_side)
    .orderBy(F.col("entity_id").desc())
)
