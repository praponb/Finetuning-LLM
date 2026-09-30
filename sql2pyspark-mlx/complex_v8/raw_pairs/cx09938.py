from pyspark.sql import functions as F

site_side = (
    df_sites
    .filter((F.year(F.col("commissioned_on")) == 2024) & (~F.col("tariff").isin("FLAT", "TOU")))
    .select(
        F.lit("site").alias("record_type"),
        F.col("id").alias("entity_id"),
        "commissioned_on",
        "site_name",
    )
)

site_side_2 = (
    df_sites
    .select(
        F.lit("site_2").alias("record_type"),
        "id",
        "commissioned_on",
        "zone",
    )
)

result = (
    site_side
    .union(site_side_2)
    .orderBy(F.col("commissioned_on").desc())
)
