from pyspark.sql import functions as F

base_sites = (
    df_sites
    .select(
        "id",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.date_add(F.col("commissioned_on"), 30).alias("commissioned_on_plus_30d"),
    )
)

site_side = base_sites.select(F.col("tariff").alias("category"))

site_side_2 = df_sites.select("tariff")

result = site_side.subtract(site_side_2)
