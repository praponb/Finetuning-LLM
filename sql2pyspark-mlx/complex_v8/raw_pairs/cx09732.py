from pyspark.sql import functions as F

tariff_side = df_tariff_windows.select("tariff")

site_side = (
    df_sites
    .filter((F.col("installed_kw").between(96.9, 446.9)) & (F.col("tariff").isin("EV", "TOU")))
    .select("tariff")
)

result = tariff_side.intersectAll(site_side)
