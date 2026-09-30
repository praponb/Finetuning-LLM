from pyspark.sql import functions as F

site_side = df_sites.select("id", "commissioned_on", F.lit("site").alias("src"))

site_side_2 = (
    df_sites
    .filter((F.col("installed_kw") <= 386.6) & (F.col("zone") == "Z2"))
    .select("id", "commissioned_on", F.lit("site_2").alias("src"))
)

site_side_3 = (
    df_sites
    .filter(F.upper(F.trim(F.col("site_name"))).like("H%"))
    .select("id", "commissioned_on", F.lit("site_3").alias("src"))
)

result = (
    site_side
    .union(site_side_2)
    .union(site_side_3)
)
