from pyspark.sql import functions as F

reading_side = df_meter_readings.select(F.col("site_id").alias("ref_id"))

tariff_rows = df_tariff_windows.select("id")

result = reading_side.intersect(tariff_rows)
