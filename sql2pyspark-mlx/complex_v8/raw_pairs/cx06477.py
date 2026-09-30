from pyspark.sql import functions as F

site_side = (
    df_sites
    .filter((F.col("installed_kw") >= 141.6) & (F.upper(F.trim(F.col("site_name"))).like("H%")))
    .select(
        F.col("site_name").alias("category"),
        F.col("installed_kw").alias("metric"),
        "tariff",
    )
)

site_rows = (
    df_sites
    .filter(F.col("installed_kw") < 284.0)
    .select("site_name", "installed_kw", "tariff")
)

combined = (
    site_side
    .union(site_rows)
    .distinct()
)

result = (
    combined
    .groupBy("category")
    .agg(F.count("*").alias("n_rows"), F.sum("metric").alias("total_metric"))
    .orderBy("category")
)
