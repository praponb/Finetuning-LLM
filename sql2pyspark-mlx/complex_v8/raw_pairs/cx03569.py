from pyspark.sql import functions as F

site_side = df_sites.select(F.col("id").alias("key_id"))

tariff_side = df_tariff_windows.select(F.col("id").alias("key_id"))

result = (
    site_side
    .exceptAll(tariff_side)
    .orderBy("key_id")
)
