from pyspark.sql import functions as F

base_sites = (
    df_sites
    .filter((F.col("commissioned_on").between(F.lit("2024-09-15").cast("date"), F.lit("2025-12-03").cast("date"))) | (F.col("zone") == "Z3"))
    .select(
        "id",
        "tariff",
        "installed_kw",
        "commissioned_on",
    )
)

site_side = (
    df_sites
    .filter((F.col("tariff") == "TOU") & (~F.col("zone").isin("Z3", "Z2")))
    .select("tariff", F.col("site_name").alias("name"))
)

site_rows = (
    df_sites
    .filter(F.col("tariff").isNotNull())
    .select(F.col("zone").alias("tariff"), F.col("site_name").alias("name"))
)

result = site_side.unionByName(site_rows)
