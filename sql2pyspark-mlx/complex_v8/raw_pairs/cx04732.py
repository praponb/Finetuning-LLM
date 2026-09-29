from pyspark.sql import functions as F

recent_sites = (
    df_sites
    .filter((~F.col("zone").isin("Z3", "Z2")) & (F.col("tariff") == "EV"))
    .select(
        "id",
        "site_name",
        "zone",
        "commissioned_on",
    )
)

site_rows = (
    recent_sites
    .filter((F.col("commissioned_on") < F.lit("2025-05-10").cast("date")) & (F.upper(F.trim(F.col("site_name"))).like("D%")))
    .select(F.lit("site").alias("source"), "zone", F.col("site_name").alias("label"))
)

site_side = (
    df_sites
    .filter(F.col("installed_kw").between(96.9, 284.0))
    .select(F.lit("site_2").alias("source"), "tariff", "site_name")
)

site_rows_2 = (
    df_sites
    .filter((F.col("tariff").isin("EV", "FLAT")) & (F.upper(F.trim(F.col("site_name"))).like("R%")))
    .select(F.lit("site_3").alias("source"), "site_name", "zone")
)

result = (
    site_rows
    .union(site_side)
    .union(site_rows_2)
)
