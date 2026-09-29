from pyspark.sql import functions as F

reading_rows = df_meter_readings.select("id")

tariff_rows = (
    df_tariff_windows
    .filter(F.col("tariff").isNotNull())
    .select("id")
)

result = (
    reading_rows
    .subtract(tariff_rows)
    .orderBy("id")
)
