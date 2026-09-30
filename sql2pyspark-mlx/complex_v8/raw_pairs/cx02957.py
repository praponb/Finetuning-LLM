from pyspark.sql import functions as F

site_side = df_sites.select("id")

tariff_rows = (
    df_tariff_windows
    .filter(F.col("rate") < 5.5)
    .select("id")
)

result = (
    site_side
    .subtract(tariff_rows)
    .orderBy(F.col("id").desc())
)
