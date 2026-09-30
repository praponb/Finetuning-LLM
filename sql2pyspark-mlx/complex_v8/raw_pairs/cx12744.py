from pyspark.sql import functions as F

filtered = (
    df_sites
    .filter(F.col("commissioned_on").isNotNull())
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.lower(F.regexp_replace(F.trim(F.col("zone")), " ", "_")).alias("zone_slug"),
    )
)

site_rows = (
    filtered
    .filter(F.col("installed_kw") <= 386.6)
    .select(F.col("id").alias("entity_id"), "commissioned_on")
)

site_side = (
    df_sites
    .filter((F.col("zone") != "Z2") & (F.col("site_name").isNotNull()))
    .select(F.col("id").alias("entity_id"), "commissioned_on")
)

result = site_rows.union(site_side)
