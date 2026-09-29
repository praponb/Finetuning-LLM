from pyspark.sql import functions as F

site_side = df_sites.select(F.col("zone").alias("category"), "id", F.lit("site").alias("source"))

reading_side = df_meter_readings.select("quality", "site_id", F.lit("reading").alias("source"))

result = (
    site_side
    .union(reading_side)
    .distinct()
)
