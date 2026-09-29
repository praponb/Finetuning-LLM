from pyspark.sql import functions as F

site_side = (
    df_sites
    .filter((F.col("installed_kw") > 96.9) & (F.col("commissioned_on").between(F.lit("2024-06-27").cast("date"), F.lit("2025-05-10").cast("date"))))
    .select(
        F.col("site_name").alias("name"),
        "commissioned_on",
        F.lit("site").alias("src"),
    )
)

site_side_2 = df_sites.select("zone", "commissioned_on", F.lit("site_2").alias("src"))

site_rows = df_sites.select("tariff", "commissioned_on", F.lit("site_3").alias("src"))

result = (
    site_side
    .union(site_side_2)
    .union(site_rows)
    .orderBy(F.col("src").desc())
)
