from pyspark.sql import functions as F

site_rows = (
    df_sites
    .filter(F.year(F.col("commissioned_on")) == 2025)
    .select("tariff", "zone", F.col("installed_kw").alias("metric"))
)

site_rows_2 = (
    df_sites
    .filter((F.lower(F.col("site_name")).like("%ea%")) & (F.col("installed_kw") > 96.9))
    .select("zone", "tariff", "installed_kw")
)

site_side = (
    df_sites
    .filter((~F.col("tariff").isin("FLAT", "EV")) & (F.col("commissioned_on") < F.lit("2025-05-07").cast("date")))
    .select("site_name", "zone", "installed_kw")
)

result = (
    site_rows
    .union(site_rows_2)
    .union(site_side)
    .orderBy("metric", "tariff")
)
