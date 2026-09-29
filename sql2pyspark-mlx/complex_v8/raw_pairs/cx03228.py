from pyspark.sql import functions as F

base = (
    df_sites
    .filter(F.col("commissioned_on").isNotNull())
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.date_trunc("week", F.col("commissioned_on")).alias("commissioned_on_week"),
    )
)

site_rows = base.select(F.col("id").alias("key_id"))

reading_rows = df_meter_readings.select("site_id")

result = (
    site_rows
    .subtract(reading_rows)
    .orderBy("key_id")
)
