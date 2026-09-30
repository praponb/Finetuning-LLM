from pyspark.sql import functions as F

reading_rows = (
    df_meter_readings
    .filter((F.col("site_id") >= 5) & (F.col("quality") == "BAD"))
    .select("site_id", "quality")
)

site_rows = df_sites.select(F.col("id").alias("site_id"), F.col("site_name").alias("quality"))

result = (
    reading_rows
    .intersect(site_rows)
    .orderBy("site_id", "quality")
)
