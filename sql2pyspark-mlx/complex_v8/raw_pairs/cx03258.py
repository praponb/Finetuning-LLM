from pyspark.sql import functions as F

filtered = (
    df_sites
    .filter((F.col("zone").isin("Z2", "Z3")) & (~F.col("tariff").isin("FLAT", "TOU")))
    .select(
        "id",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        (F.floor(F.col("installed_kw") / 100) * 100).alias("installed_kw_bucket"),
    )
)

site_side = (
    filtered
    .select(
        "installed_kw_bucket",
        "commissioned_on",
        "zone",
        F.lit("site").alias("src"),
    )
)

site_rows = (
    df_sites
    .select(
        "id",
        "commissioned_on",
        "zone",
        F.lit("site_2").alias("src"),
    )
)

stacked = site_side.union(site_rows)

result = (
    stacked
    .groupBy("src")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("installed_kw_bucket").alias("total_installed_kw_bucket"),
        F.max("commissioned_on").alias("latest_commissioned_on"),
    )
    .orderBy("src")
)
