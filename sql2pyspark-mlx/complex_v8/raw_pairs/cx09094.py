from pyspark.sql import functions as F

base = (
    df_sites
    .filter((F.col("tariff").isin("EV", "FLAT")) & (F.col("installed_kw") <= 373.3))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.trunc(F.col("commissioned_on"), "MM").alias("commissioned_on_month_start"),
    )
)

site_rows = (
    base
    .filter((F.col("commissioned_on_month_start").between(F.lit("2024-05-01").cast("date"), F.lit("2025-10-01").cast("date"))) & (F.col("commissioned_on") < F.lit("2024-11-21").cast("date")))
    .select("id", F.col("installed_kw").alias("value"))
)

site_side = df_sites.select("id", F.col("installed_kw").alias("value"))

reading_side = (
    df_meter_readings
    .filter(F.col("quality") == "BAD")
    .select(F.col("site_id").alias("id"), F.col("voltage").alias("value"))
)

result = (
    site_rows
    .union(site_side)
    .union(reading_side)
    .orderBy("value")
)
