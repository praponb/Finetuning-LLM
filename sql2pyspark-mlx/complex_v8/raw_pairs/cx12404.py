from pyspark.sql import functions as F

site_side = (
    df_sites
    .select(
        F.col("id").alias("entity_id"),
        F.col("site_name").alias("name"),
        "commissioned_on",
        F.lit("site").alias("record_type"),
    )
)

site_side_2 = (
    df_sites
    .select(
        "id",
        "zone",
        "commissioned_on",
        F.lit("site_2").alias("record_type"),
    )
)

site_rows = (
    df_sites
    .filter(F.year(F.col("commissioned_on")) == 2024)
    .select(
        "id",
        "zone",
        "commissioned_on",
        F.lit("site_3").alias("record_type"),
    )
)

result = (
    site_side
    .union(site_side_2)
    .union(site_rows)
    .orderBy(F.col("entity_id").desc())
)
