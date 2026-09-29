from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w_part = Window.partitionBy("device_code")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(-2, -1)

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") > F.to_timestamp(F.lit("2025-09-01 06:00:00")))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.initcap(F.trim("firmware")).alias("firmware_title"),
    )
)

result = (
    filtered
    .filter(F.col("firmware").isin(["1.1.0", "1.0.3"]))
    .select(
        "id",
        "reading_ts",
        "humidity",
        "pressure",
        "firmware",
        F.sum("humidity").over(w).alias("humidity_sum_30m"),
        F.min("pressure").over(w_part).alias("device_code_min_pressure"),
        F.avg("humidity").over(w3).alias("humidity_avg_2_prior"),
    )
)
