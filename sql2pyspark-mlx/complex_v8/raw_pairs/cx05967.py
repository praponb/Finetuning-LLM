from pyspark.sql import functions as F

site_rows = (
    df_sites
    .select(
        F.col("id").alias("entity_id"),
        "commissioned_on",
        F.lit("site").alias("source"),
    )
)

site_side = (
    df_sites
    .filter(F.col("zone").isin("Z4", "Z3"))
    .select("id", "commissioned_on", F.lit("site_2").alias("source"))
)

site_side_2 = (
    df_sites
    .filter((F.col("zone") == "Z3") & (F.col("commissioned_on").isNotNull()))
    .select("id", "commissioned_on", F.lit("site_3").alias("source"))
)

result = (
    site_rows
    .union(site_side)
    .union(site_side_2)
    .orderBy("entity_id", "source")
)
