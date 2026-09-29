from pyspark.sql import functions as F

reading_side = (
    df_meter_readings
    .filter((F.col("kwh") < 86.0) & (F.col("read_at") < F.lit("2025-03-08 10:00:00").cast("timestamp")))
    .select("quality", F.col("read_at").alias("happened_at"))
)

reading_rows = (
    df_meter_readings
    .filter(F.col("quality").isin("BAD", "OK"))
    .select("quality", F.col("read_at").alias("happened_at"))
)

result = (
    reading_side
    .unionByName(reading_rows)
    .distinct()
)
