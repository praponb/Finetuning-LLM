from pyspark.sql import functions as F

reading_rows = (
    df_meter_readings
    .select(F.col("quality").alias("label"), F.col("site_id").alias("qty"))
)

site_side = df_sites.select("site_name", "id")

site_side_2 = df_sites.select("tariff", "id")

result = (
    reading_rows
    .union(site_side)
    .union(site_side_2)
    .orderBy("qty", "label")
)
