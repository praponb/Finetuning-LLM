from pyspark.sql import functions as F

tariff_side = df_tariff_windows.select(F.col("tariff").alias("name"), F.col("rate").alias("value"))

reading_rows = df_meter_readings.select(F.col("quality").alias("name"), F.col("kwh").alias("value"))

combined = tariff_side.unionByName(reading_rows)

result = (
    combined
    .groupBy("name")
    .agg(F.count("*").alias("n_rows"), F.sum("value").alias("total_value"))
    .orderBy("name")
)
