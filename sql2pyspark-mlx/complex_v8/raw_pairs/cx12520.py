from pyspark.sql import functions as F

base_sites = (
    df_sites
    .filter((F.col("zone") != "Z3") & (F.col("tariff").isNotNull()))
    .select(
        "id",
        "zone",
        "tariff",
        "commissioned_on",
    )
)

site_side = (
    base_sites
    .filter(F.col("commissioned_on") < F.lit("2025-05-10").cast("date"))
    .select("id")
)

reading_rows = df_meter_readings.select("id")

result = (
    site_side
    .subtract(reading_rows)
    .orderBy(F.col("id").desc())
)
