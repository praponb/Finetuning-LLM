from pyspark.sql import functions as F

base = (
    df_sites
    .filter((F.col("commissioned_on") >= F.lit("2024-08-24").cast("date")) & (F.col("tariff") == "EV"))
)

site_rows = (
    base
    .filter(F.col("site_name").isNotNull())
    .select(F.lit("site").alias("origin"), "id", "commissioned_on")
)

site_side = (
    df_sites
    .filter((F.lower(F.col("site_name")).like("%ea%")) & (F.col("installed_kw") >= 141.6))
    .select(F.lit("site_2").alias("origin"), "id", "commissioned_on")
)

result = (
    site_rows
    .union(site_side)
    .distinct()
    .orderBy("origin", "id")
)
