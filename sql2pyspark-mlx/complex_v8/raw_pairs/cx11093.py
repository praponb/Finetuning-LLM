from pyspark.sql import functions as F

tariff_rows = (
    df_tariff_windows
    .filter((F.col("tariff") == "EV") & (F.month(F.col("valid_from")).isin(2, 3)))
    .select(F.col("tariff").alias("name"))
)

reading_side = df_meter_readings.select("quality")

result = (
    tariff_rows
    .subtract(reading_side)
    .orderBy(F.col("name").desc())
)
