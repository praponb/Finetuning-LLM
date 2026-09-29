from pyspark.sql import functions as F

base = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01").cast("date")) & (F.col("pressure") <= 1002))
    .withColumn("reading_ts_day", F.col("reading_ts").cast("date"))
)

result = (
    base
    .select("firmware")
    .intersect(
        df_device_readings
        .filter(F.col("firmware") == "1.0.3")
        .select("firmware")
    )
)
