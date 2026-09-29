from pyspark.sql import functions as F

tariff_rows = df_tariff_windows.select("id")

reading_rows = df_meter_readings.select("id")

result = (
    tariff_rows
    .intersect(reading_rows)
    .orderBy("id")
)
