from pyspark.sql import functions as F

tariff_side = df_tariff_windows.select("tariff")

site_side = df_sites.select("tariff")

result = (
    tariff_side
    .subtract(site_side)
    .orderBy(F.col("tariff").desc())
)
