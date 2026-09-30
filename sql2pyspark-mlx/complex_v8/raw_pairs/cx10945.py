from pyspark.sql import functions as F

reading_side = df_meter_readings.select("id", "quality")

site_rows = (
    df_sites
    .filter(F.col("installed_kw") < 284.0)
    .select("id", "tariff")
)

result = (
    reading_side
    .intersectAll(site_rows)
    .orderBy("quality", "id")
)
