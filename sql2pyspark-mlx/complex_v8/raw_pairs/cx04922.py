from pyspark.sql import functions as F

site_rows = (
    df_sites
    .filter((F.col("zone") == "Z4") & (F.col("installed_kw").between(93.3, 284.0)))
    .select(
        F.lit("site").alias("source"),
        "tariff",
        "commissioned_on",
        F.col("zone").alias("name"),
    )
)

site_side = (
    df_sites
    .filter((F.col("commissioned_on") < F.lit("2024-11-21").cast("date")) & (F.col("installed_kw") < 373.3))
    .select(
        F.lit("site_2").alias("source"),
        "tariff",
        "commissioned_on",
        "site_name",
    )
)

site_side_2 = (
    df_sites
    .filter(F.col("zone").isin("Z2", "Z3"))
    .select(
        F.lit("site_3").alias("source"),
        "site_name",
        "commissioned_on",
        "tariff",
    )
)

result = (
    site_rows
    .union(site_side)
    .union(site_side_2)
    .orderBy(F.col("commissioned_on").desc(), "name")
)
