from pyspark.sql import functions as F

site_side = (
    df_sites
    .filter((F.upper(F.trim(F.col("site_name"))).like("S%")) & (F.col("installed_kw") > 141.6))
    .select(F.col("zone").alias("category"), "tariff")
)

site_side_2 = df_sites.select("site_name", "zone")

unioned = site_side.union(site_side_2)

result = (
    unioned
    .groupBy("category")
    .agg(F.count("*").alias("n_rows"))
    .orderBy("category")
)
