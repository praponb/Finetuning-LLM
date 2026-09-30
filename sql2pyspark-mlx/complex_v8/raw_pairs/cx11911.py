from pyspark.sql import functions as F

reading_side = df_meter_readings.select("site_id")

tariff_side = df_tariff_windows.select("id")

result = (
    reading_side
    .intersect(tariff_side)
    .orderBy(F.col("site_id").desc())
)
