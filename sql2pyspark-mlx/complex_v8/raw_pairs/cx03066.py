from pyspark.sql import functions as F

recent = (
    df_sites
    .filter((F.col("site_name").isNotNull()) & (F.col("zone") != "Z4"))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
    )
)

site_rows = (
    recent
    .filter(F.upper(F.trim(F.col("site_name"))).like("R%"))
    .select("id")
)

reading_side = df_meter_readings.select("site_id")

result = site_rows.subtract(reading_side)
