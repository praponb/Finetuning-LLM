from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.year("reading_ts")).orderBy(F.col("pressure").desc(), "id")
w3 = Window.partitionBy("device_code")
w4 = Window.partitionBy("device_code").orderBy(F.col("running_sum_temp_c").desc(), "id")
w5 = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("device_code").orderBy("running_min_temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("device_code").orderBy(F.col("humidity_share").desc_nulls_first(), "id")
w8 = Window.partitionBy("device_code").orderBy(F.col("rank_in_group").desc(), "id")
w9 = Window.partitionBy("device_code").orderBy("running_sum_temp_c_bucket", "id")
w10 = Window.partitionBy("device_code").orderBy(F.col("running_min_temp_c").asc_nulls_last(), "id")

with_windows = (
    df_device_readings
    .select(
        "*",
        F.count("temp_c").over(w).alias("running_count_temp_c"),
        F.sum("temp_c").over(w).alias("running_sum_temp_c"),
        F.min("temp_c").over(w).alias("running_min_temp_c"),
        F.ntile(10).over(w2).alias("pressure_bucket"),
    )
)

enriched = (
    with_windows
    .filter(F.col("pressure") <= 1013)
    .select(
        "id",
        "device_code",
        "temp_c",
        "firmware",
        "running_count_temp_c",
        F.round((F.col("humidity") / F.sum("humidity").over(w3)), 4).alias("humidity_share"),
        F.ntile(3).over(w4).alias("running_sum_temp_c_bucket"),
        F.round((F.col("pressure") / F.sum("pressure").over(w3)), 4).alias("pressure_share"),
        F.sum("humidity").over(w5).alias("cumulative_humidity"),
        F.sum("running_min_temp_c").over(w6).alias("running_running_min_temp_c"),
    )
)

latest = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w7))
    .filter(F.col("rank_in_group") == 1)
)

latest_device_readings = (
    latest
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_windows_device_readings = (
    latest_device_readings
    .withColumn("running_sum_temp_c", F.sum("temp_c").over(w5))
    .withColumn("running_min_temp_c", F.min("temp_c").over(w5))
    .withColumn("running_max_temp_c", F.max("temp_c").over(w5))
    .withColumn("running_sum_temp_c_bucket_bucket", F.ntile(5).over(w9))
)

latest_device_readings_2 = (
    with_windows_device_readings
    .withColumn("latest_rank", F.row_number().over(w10))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    latest_device_readings_2
    .select(
        "id",
        "device_code",
        "temp_c",
        "firmware",
        "running_sum_temp_c_bucket",
        "pressure_share",
        "running_running_min_temp_c",
        "rank_in_group",
        "running_min_temp_c",
    )
    .orderBy(F.col("temp_c").asc_nulls_first(), F.col("device_code").desc())
)
