from pyspark.sql import functions as F

reading_side = (
    df_meter_readings
    .filter(F.col("read_at") >= F.lit("2025-01-19 13:00:00").cast("timestamp"))
    .select("id", "read_at", F.lit("reading").alias("source"))
)

reading_side_2 = (
    df_meter_readings
    .filter((F.col("kwh") > 50.4) & (F.col("voltage") <= 236.7))
    .select("id", "read_at", F.lit("reading_2").alias("source"))
)

reading_rows = df_meter_readings.select("id", "read_at", F.lit("reading_3").alias("source"))

result = (
    reading_side
    .unionByName(reading_side_2)
    .unionByName(reading_rows)
)
