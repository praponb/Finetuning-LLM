from pyspark.sql import functions as F

site_rows = df_sites.select(F.col("id").alias("entity_id"), "installed_kw", "commissioned_on")

site_rows_2 = (
    df_sites
    .filter(F.col("tariff").isNotNull())
    .select("id", "installed_kw", "commissioned_on")
)

result = (
    site_rows
    .union(site_rows_2)
    .distinct()
    .orderBy(F.col("commissioned_on").desc())
)
