from pyspark.sql import functions as F

filtered_sites = (
    df_sites
    .filter((F.col("zone").isNotNull()) & (F.col("installed_kw").between(93.3, 373.3)))
    .select(
        "id",
        "zone",
        "installed_kw",
        "commissioned_on",
        F.lower(F.regexp_replace(F.trim(F.col("tariff")), " ", "_")).alias("tariff_slug"),
    )
)

site_side = (
    filtered_sites
    .filter(F.col("zone").isin("Z4", "Z3"))
    .select(F.col("id").alias("entity_id"))
)

site_rows = (
    df_sites
    .filter(F.col("tariff") != "TOU")
    .select("id")
)

result = (
    site_side
    .subtract(site_rows)
    .orderBy(F.col("entity_id").desc())
)
