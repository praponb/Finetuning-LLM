from pyspark.sql import functions as F

site_rows = (
    df_sites
    .filter((F.col("tariff") == "TOU") & (F.col("installed_kw") >= 93.3))
    .select(
        "site_name",
        F.col("commissioned_on").alias("day"),
        F.col("id").alias("qty"),
    )
)

site_side = df_sites.select("site_name", "commissioned_on", "id")

site_side_2 = (
    df_sites
    .filter(F.col("installed_kw") > 96.9)
    .select("tariff", "commissioned_on", "id")
)

result = (
    site_rows
    .union(site_side)
    .union(site_side_2)
    .orderBy(F.col("site_name").desc(), "qty")
)
