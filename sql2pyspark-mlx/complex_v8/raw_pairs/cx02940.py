from pyspark.sql import functions as F

site_side = (
    df_sites
    .filter((F.upper(F.trim(F.col("site_name"))).like("S%")) & (F.col("zone").isin("Z2", "Z3")))
    .select("id")
)

site_side_2 = df_sites.select("id")

result = (
    site_side
    .subtract(site_side_2)
    .orderBy("id")
)
