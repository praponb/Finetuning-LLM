from pyspark.sql import functions as F

valid_sites = df_sites

site_side = (
    valid_sites
    .filter((F.col("tariff") == "FLAT") & (F.col("site_name").isNotNull()))
    .select(
        F.col("id").alias("entity_id"),
        F.col("commissioned_on").alias("day"),
        "site_name",
    )
)

site_side_2 = (
    df_sites
    .filter((F.col("installed_kw").between(96.9, 386.6)) & (F.col("tariff") == "EV"))
    .select("id", "commissioned_on", "zone")
)

site_rows = (
    df_sites
    .filter((F.col("tariff") == "FLAT") & (~F.col("zone").isin("Z3", "Z4")))
    .select("id", "commissioned_on", "zone")
)

result = (
    site_side
    .union(site_side_2)
    .union(site_rows)
)
