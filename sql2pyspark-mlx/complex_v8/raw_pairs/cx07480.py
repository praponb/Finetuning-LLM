from pyspark.sql import functions as F

site_side = df_sites.select(F.col("id").alias("ref_id"))

tariff_side = (
    df_tariff_windows
    .filter(F.col("tariff").isin("TOU", "EV"))
    .select("id")
)

result = (
    site_side
    .subtract(tariff_side)
    .orderBy("ref_id")
)
