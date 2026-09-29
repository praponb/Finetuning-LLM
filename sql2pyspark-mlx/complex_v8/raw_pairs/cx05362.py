from pyspark.sql import functions as F

valid = (
    df_sites
    .filter(~F.col("tariff").isin("TOU", "EV"))
    .select(
        "id",
        "site_name",
        "tariff",
        "installed_kw",
        "commissioned_on",
    )
)

site_side = (
    valid
    .filter((F.col("commissioned_on") < F.lit("2025-05-10").cast("date")) & (F.col("installed_kw") >= 96.9))
    .select(
        F.lit("site").alias("src"),
        F.col("id").alias("entity_id"),
        "installed_kw",
        "commissioned_on",
    )
)

site_rows = (
    df_sites
    .filter(F.col("zone") == "Z4")
    .select(
        F.lit("site_2").alias("src"),
        "id",
        "installed_kw",
        "commissioned_on",
    )
)

site_rows_2 = (
    df_sites
    .filter((F.col("tariff").isin("TOU", "FLAT")) & (F.col("zone") == "Z4"))
    .select(
        F.lit("site_3").alias("src"),
        "id",
        "installed_kw",
        "commissioned_on",
    )
)

result = (
    site_side
    .union(site_rows)
    .union(site_rows_2)
    .orderBy("entity_id")
)
