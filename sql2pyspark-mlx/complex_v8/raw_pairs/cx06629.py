from pyspark.sql import functions as F

site_rows = (
    df_sites
    .filter(F.col("commissioned_on") < F.lit("2024-11-21").cast("date"))
    .select(
        "id",
        F.col("tariff").alias("category"),
        F.lit("site").alias("record_type"),
    )
)

site_side = (
    df_sites
    .filter((F.upper(F.trim(F.col("site_name"))).like("S%")) & (F.col("commissioned_on").isNotNull()))
    .select("id", "zone", F.lit("site_2").alias("record_type"))
)

result = site_rows.union(site_side)
