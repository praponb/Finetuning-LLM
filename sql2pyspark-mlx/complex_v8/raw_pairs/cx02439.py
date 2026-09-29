from pyspark.sql import functions as F

site_rows = (
    df_sites
    .filter(F.col("site_name").isNotNull())
    .select(F.col("id").alias("ref_id"), F.col("commissioned_on").alias("day"))
)

site_rows_2 = (
    df_sites
    .filter((F.col("zone") == "Z3") & (F.col("installed_kw") < 373.3))
    .select("id", "commissioned_on")
)

result = (
    site_rows
    .union(site_rows_2)
    .orderBy(F.col("ref_id").desc(), "day")
)
