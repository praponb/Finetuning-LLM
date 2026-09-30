from pyspark.sql import functions as F

reading_rows = (
    df_meter_readings
    .filter((F.col("voltage") > 216.8) & (F.col("kwh") > 48.2))
    .select("id", "read_at")
)

reading_rows_2 = df_meter_readings.select("site_id", "read_at")

stacked = (
    reading_rows
    .union(reading_rows_2)
    .distinct()
)

result = (
    stacked
    .groupBy("id")
    .agg(F.count("*").alias("n_rows"), F.max("read_at").alias("latest_read_at"))
    .orderBy("id")
)
