from pyspark.sql import functions as F

site_side = df_sites.select(F.col("id").alias("qty"))

tariff_side = (
    df_tariff_windows
    .filter((F.col("tariff") == "EV") & (F.hour(F.col("valid_from")).between(10, 15)))
    .select(F.col("id").alias("qty"))
)

result = (
    site_side
    .intersect(tariff_side)
    .orderBy("qty")
)
