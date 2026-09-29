from pyspark.sql import functions as F

cleaned = (
    df_sites
    .filter(F.col("commissioned_on").between(F.lit("2024-08-24").cast("date"), F.lit("2025-12-03").cast("date")))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
    )
)

site_side = cleaned.select("id", F.col("zone").alias("name"))

site_rows = df_sites.select("id", F.col("zone").alias("name"))

result = (
    site_side
    .intersect(site_rows)
    .orderBy(F.col("name").desc(), "id")
)
