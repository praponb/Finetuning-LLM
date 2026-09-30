from pyspark.sql import functions as F

reading_side = df_meter_readings.select("id", F.col("kwh").alias("value"))

reading_side_2 = (
    df_meter_readings
    .filter(F.col("quality").isin("EST", "OK"))
    .select("site_id", "kwh")
)

stacked = reading_side.union(reading_side_2)

result = (
    stacked
    .groupBy("id")
    .agg(F.count("*").alias("n_rows"), F.sum("value").alias("total_value"))
    .orderBy("id")
)
