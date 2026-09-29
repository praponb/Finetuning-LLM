from pyspark.sql import functions as F

site_side = (
    df_sites
    .filter((F.lower(F.col("site_name")).like("%al%")) & (F.col("tariff").isin("EV", "FLAT")))
    .select("id")
)

reading_side = df_meter_readings.select("site_id")

result = site_side.intersect(reading_side)
