from pyspark.sql import functions as F

cleaned = (
    df_sites
    .filter((F.col("installed_kw") >= 141.6) & (F.col("tariff").isin("FLAT", "EV")))
    .select(
        "id",
        "site_name",
        "zone",
        "installed_kw",
        "commissioned_on",
    )
)

site_side = (
    cleaned
    .filter((F.col("installed_kw") < 373.3) & (F.col("commissioned_on") >= F.lit("2024-08-24").cast("date")))
    .select(F.col("id").alias("entity_id"))
)

site_rows = (
    df_sites
    .filter(F.col("installed_kw") >= 141.6)
    .select(F.col("id").alias("entity_id"))
)

result = (
    site_side
    .intersectAll(site_rows)
    .orderBy(F.col("entity_id").desc())
)
