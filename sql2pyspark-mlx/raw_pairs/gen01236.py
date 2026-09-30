from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w4 = Window.orderBy(F.col("status_bits_avg_1h").desc_nulls_last())
w5 = Window.orderBy("status_bits_avg_1h", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.to_date("reading_ts")).orderBy("status_bits", "id").rowsBetween(-6, -1)
w_recent = Window.orderBy("running_running_max_humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_device_readings
    .select(
        "id",
        "reading_ts",
        "pressure",
        "status_bits",
        "firmware",
        F.min("humidity").over(w).alias("running_min_humidity"),
        F.max("humidity").over(w).alias("running_max_humidity"),
        F.avg("humidity").over(w).alias("running_avg_humidity"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "reading_ts",
        "pressure",
        "status_bits",
        "firmware",
        "running_min_humidity",
        "running_max_humidity",
        F.sum("running_avg_humidity").over(w2).alias("running_avg_humidity_sum_15m"),
        F.avg("status_bits").over(w3).alias("status_bits_avg_1h"),
    )
)

enriched_device_readings = (
    metrics
    .filter(F.col("pressure") < 1022)
    .select(
        "*",
        F.rank().over(w4).alias("status_bits_avg_1h_rank"),
        F.sum("running_max_humidity").over(w5).alias("running_running_max_humidity"),
        F.avg("running_max_humidity").over(w6).alias("running_max_humidity_avg_6_prior"),
    )
)

base = (
    enriched_device_readings
    .filter(F.col("firmware") == "1.1.0")
    .select("*", F.upper("firmware").alias("firmware_upper"))
)

result = (
    base
    .select(
        "id",
        "reading_ts",
        "pressure",
        "firmware",
        "running_min_humidity",
        "running_max_humidity",
        "running_avg_humidity_sum_15m",
        "status_bits_avg_1h",
        "running_running_max_humidity",
        "running_max_humidity_avg_6_prior",
        "firmware_upper",
        F.max("status_bits_avg_1h_rank").over(w_recent).alias("running_max_status_bits_avg_1h_rank"),
        F.avg("status_bits_avg_1h_rank").over(w_recent).alias("running_avg_status_bits_avg_1h_rank"),
        F.sum("status_bits_avg_1h_rank").over(w_recent).alias("running_sum_status_bits_avg_1h_rank"),
    )
)
