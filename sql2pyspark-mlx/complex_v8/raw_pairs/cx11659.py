from pyspark.sql import functions as F

reading_side = df_meter_readings.select(F.col("quality").alias("label"))

tariff_side = df_tariff_windows.select(F.col("tariff").alias("label"))

result = reading_side.subtract(tariff_side)
