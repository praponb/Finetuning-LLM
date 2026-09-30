from pyspark.sql import functions as F

site_rows = (
    df_sites
    .filter(F.col("installed_kw") < 284.0)
    .select(F.col("zone").alias("category"))
)

site_rows_2 = (
    df_sites
    .filter((F.length(F.trim(F.col("site_name"))) >= 5) & (~F.col("tariff").isin("FLAT", "EV")))
    .select(F.col("site_name").alias("category"))
)

result = (
    site_rows
    .subtract(site_rows_2)
    .orderBy("category")
)
