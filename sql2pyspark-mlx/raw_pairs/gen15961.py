from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc(), "id")
w2 = Window.partitionBy("device_code").orderBy("pressure", "id")
w_recent = Window.partitionBy("device_code")
w4 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy(F.date_trunc("WEEK", "reading_ts"), "device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy(F.month("reading_ts"))
w8 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w9 = Window.orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("corr_temp_c_status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.orderBy("humidity_sum_1h", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

deduped = (
    df_device_readings
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

latest = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    latest
    .select(
        "*",
        F.max("status_bits").over(w_recent).alias("device_code_max_status_bits"),
        F.count("pressure").over(w_recent).alias("device_code_count_pressure"),
        F.max("humidity").over(w4).alias("humidity_max_2h"),
        F.sum("pressure").over(w5).alias("running_pressure"),
        F.corr("temp_c", "status_bits").over(w_recent).alias("corr_temp_c_status_bits"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        "latest_rank",
        "device_code_max_status_bits",
        "corr_temp_c_status_bits",
        F.sum("humidity_max_2h").over(w_part).alias("cumulative_humidity_max_2h"),
        F.corr("temp_c", "humidity").over(w7).alias("corr_temp_c_humidity"),
        F.sum("humidity").over(w8).alias("humidity_sum_1h"),
        F.first("humidity_max_2h", ignorenulls=True).over(w9).alias("first_known_humidity_max_2h"),
    )
)

with_windows = (
    metrics
    .filter(F.col("corr_temp_c_humidity") <= -0.4)
    .withColumn("last_known_temp_c", F.last("temp_c", ignorenulls=True).over(w10))
    .withColumn("running_device_code_max_status_bits", F.sum("device_code_max_status_bits").over(w11))
    .withColumn("first_known_cumulative_humidity_max_2h", F.first("cumulative_humidity_max_2h", ignorenulls=True).over(w12))
)

result = (
    with_windows
    .select(
        "id",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        "latest_rank",
        "device_code_max_status_bits",
        "corr_temp_c_status_bits",
        "cumulative_humidity_max_2h",
    )
    .orderBy("corr_temp_c_status_bits")
)
