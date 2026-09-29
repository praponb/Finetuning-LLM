from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("pressure_rounded").desc(), "id")
w_ordered = Window.partitionBy("device_code").orderBy("pressure_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("pressure_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("pressure_rounded", "id")
w5 = Window.orderBy("humidity_times_temp_c", "id")
w6 = Window.partitionBy("device_code").orderBy("running_avg_humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 07:00:00").cast("timestamp"))
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.round("pressure", 0).alias("pressure_rounded"),
        (F.col("humidity") * F.col("temp_c")).alias("humidity_times_temp_c"),
    )
)

latest = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

metrics = (
    latest
    .withColumn("running_max_humidity", F.max("humidity").over(w_ordered))
    .withColumn("running_count_humidity", F.count("humidity").over(w_ordered))
    .withColumn("running_avg_humidity", F.avg("humidity").over(w_ordered))
    .withColumn("running_sum_humidity", F.sum("humidity").over(w_ordered))
)

with_windows = (
    metrics
    .withColumn("running_running_max_humidity", F.sum("running_max_humidity").over(w3))
    .withColumn("seq_no", F.row_number().over(w4))
    .withColumn("event_seq", F.row_number().over(w5))
    .withColumn("cumulative_running_sum_humidity", F.sum("running_sum_humidity").over(w6))
)

result = (
    with_windows
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        "pressure_rounded",
        "humidity_times_temp_c",
        "row_num",
    )
)
