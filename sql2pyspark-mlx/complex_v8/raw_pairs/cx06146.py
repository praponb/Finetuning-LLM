from pyspark.sql import functions as F

tariff_side = df_tariff_windows.select("tariff")

site_rows = df_sites.select("zone")

result = (
    tariff_side
    .intersectAll(site_rows)
    .orderBy("tariff")
)
