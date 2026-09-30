from pyspark.sql import functions as F

reading_side = (
    df_meter_readings
    .filter((F.col("site_id") <= 3) & (F.hour(F.col("read_at")).between(6, 9)))
    .select(F.col("site_id").alias("key_id"))
)

site_side = (
    df_sites
    .filter(F.col("installed_kw") <= 373.3)
    .select("id")
)

result = (
    reading_side
    .subtract(site_side)
    .orderBy(F.col("key_id").desc())
)
