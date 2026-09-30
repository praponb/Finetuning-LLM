from pyspark.sql import functions as F

cleaned = (
    df_sites
    .filter(F.year(F.col("commissioned_on")) == 2024)
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
    )
)

site_side = cleaned.select(F.lit("site").alias("record_type"), "id", "commissioned_on")

site_side_2 = df_sites.select(F.lit("site_2").alias("record_type"), "id", "commissioned_on")

result = (
    site_side
    .union(site_side_2)
    .orderBy(F.col("record_type").desc())
)
