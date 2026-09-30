from pyspark.sql import functions as F

site_rows = (
    df_sites
    .filter(F.col("installed_kw").between(96.9, 386.6))
    .select("id", F.col("tariff").alias("label"), "installed_kw")
)

tariff_side = df_tariff_windows.select("id", "tariff", "rate")

result = site_rows.union(tariff_side)
