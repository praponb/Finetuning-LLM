from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("pressure").desc(), "id")
w2 = Window.partitionBy("device_code").orderBy("status_bits", "id")
w3 = Window.orderBy("reading_ts", "id").rowsBetween(-3, Window.currentRow)

with_windows = (
    df_device_readings
    .select(
        "id",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        F.ntile(10).over(w).alias("pressure_bucket"),
        F.row_number().over(w2).alias("row_seq"),
        F.avg("humidity").over(w3).alias("humidity_avg_3"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("reading_ts") > F.lit("2025-09-01 03:00:00").cast("timestamp"))
    .select(
        "id",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "pressure_bucket",
        "row_seq",
        "humidity_avg_3",
        F.round("humidity", 1).alias("humidity_rounded"),
    )
)

valid = (
    cleaned
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .withColumn("humidity_whole", F.ceil("humidity"))
)

valid_device_readings = (
    valid
    .filter(F.col("pressure_bucket").between(7, 8))
    .select(
        "*",
        F.when(F.col("pressure") >= 1019, "High").when(F.col("pressure") >= 999, "Medium").otherwise("Low").alias("pressure_tier"),
        F.round((F.col("row_seq") / F.col("humidity")), 2).alias("row_seq_per_humidity"),
    )
)

result = (
    valid_device_readings
    .select(
        "id",
        "reading_ts",
        "humidity",
        "pressure",
        "row_seq",
        "humidity_rounded",
        "humidity_whole",
        "pressure_tier",
    )
)
