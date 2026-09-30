from pyspark.sql import functions as F

tariff_rows = (
    df_tariff_windows
    .filter(F.hour(F.col("valid_from")).between(10, 14))
    .select("tariff")
)

site_side = df_sites.select(F.col("zone").alias("tariff"))

result = (
    tariff_rows
    .subtract(site_side)
    .orderBy("tariff")
)
