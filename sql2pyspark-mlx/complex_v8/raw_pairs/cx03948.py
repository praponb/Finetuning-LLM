from pyspark.sql import functions as F

site_rows = (
    df_sites
    .filter((F.col("installed_kw") <= 373.3) & (F.length(F.trim(F.col("site_name"))) >= 5))
    .select("zone", "commissioned_on", "id")
)

site_side = (
    df_sites
    .filter((F.col("installed_kw") <= 284.0) & (~F.col("zone").isin("Z2", "Z3")))
    .select("tariff", "commissioned_on", "id")
)

result = (
    site_rows
    .union(site_side)
    .distinct()
    .orderBy(F.col("zone").desc(), "commissioned_on")
)
