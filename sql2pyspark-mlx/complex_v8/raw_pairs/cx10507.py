from pyspark.sql import functions as F

tariff_rows = (
    df_tariff_windows
    .filter((F.col("rate") <= 5.7) & (F.col("tariff").isin("TOU", "FLAT")))
    .select("id", "tariff")
)

site_rows = (
    df_sites
    .filter((F.col("commissioned_on") >= F.lit("2024-09-15").cast("date")) & (F.col("zone").isin("Z2", "Z3")))
    .select("id", "site_name")
)

result = tariff_rows.union(site_rows)
