from pyspark.sql import functions as F

reading_side = (
    df_meter_readings
    .filter((F.col("quality") != "BAD") & (F.col("voltage") >= 218.5))
    .select(F.col("site_id").alias("qty"))
)

site_side = df_sites.select("id")

result = (
    reading_side
    .intersect(site_side)
    .orderBy(F.col("qty").desc())
)
