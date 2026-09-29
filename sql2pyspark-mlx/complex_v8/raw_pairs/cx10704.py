from pyspark.sql import functions as F

valid_sites = (
    df_sites
    .filter(F.col("installed_kw") < 386.6)
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
    )
)

site_side = (
    valid_sites
    .filter(F.col("installed_kw") <= 373.3)
    .select("id", "tariff")
)

reading_rows = df_meter_readings.select("id", F.col("quality").alias("tariff"))

result = (
    site_side
    .intersect(reading_rows)
    .orderBy("tariff", "id")
)
