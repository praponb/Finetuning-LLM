from pyspark.sql import functions as F

site_rows = df_sites.select("zone", "installed_kw", F.col("tariff").alias("name"))

site_side = (
    df_sites
    .filter((F.upper(F.trim(F.col("site_name"))).like("R%")) & (F.col("installed_kw") > 141.6))
    .select(
        F.col("site_name").alias("zone"),
        "installed_kw",
        F.col("tariff").alias("name"),
    )
)

unioned = site_rows.union(site_side)

result = (
    unioned
    .groupBy("zone")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("installed_kw").alias("total_installed_kw"),
    )
    .orderBy("zone")
)
