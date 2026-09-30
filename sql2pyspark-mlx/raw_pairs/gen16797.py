from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.partitionBy(F.month("reading_ts"), "device_code").orderBy(F.col("temp_c_per_status_bits").desc_nulls_last())
w3 = Window.orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.year("reading_ts"), "device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("device_code").orderBy(F.col("last_known_humidity_2").desc(), "id")
w7 = Window.partitionBy("device_code").orderBy(F.col("cumulative_humidity").desc(), "id")
w8 = Window.partitionBy("device_code").orderBy("last_known_humidity_minus_cumulative_humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_device_readings
    .filter(F.hour("reading_ts").between(11, 16))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        (F.col("temp_c") / F.nullif("status_bits", F.lit(0))).alias("temp_c_per_status_bits"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "device_code",
        "humidity",
        "temp_c_per_status_bits",
        F.count("humidity").over(w).alias("device_code_count_humidity"),
        F.cume_dist().over(w2).alias("temp_c_per_status_bits_cume_dist"),
        F.last("humidity", ignorenulls=True).over(w3).alias("last_known_humidity"),
        F.sum("humidity").over(w4).alias("cumulative_humidity"),
        F.last("humidity", ignorenulls=True).over(w5).alias("last_known_humidity_2"),
    )
)

latest = (
    with_windows
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("id", "device_code", "last_known_humidity", "cumulative_humidity")
)

base = (
    latest
    .filter(F.col("last_known_humidity") <= 68)
    .withColumn("last_known_humidity_minus_cumulative_humidity", (F.col("last_known_humidity") - F.col("cumulative_humidity")))
)

ranked = (
    base
    .withColumn("rank_in_group", F.row_number().over(w7))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .select(
        "id",
        "device_code",
        "last_known_humidity",
        "cumulative_humidity",
        "last_known_humidity_minus_cumulative_humidity",
        "rank_in_group",
        F.avg("last_known_humidity_minus_cumulative_humidity").over(w8).alias("running_avg_last_known_humidity_minus_cumulative_humidity"),
        F.count("last_known_humidity_minus_cumulative_humidity").over(w8).alias("running_count_last_known_humidity_minus_cumulative_humidity"),
        F.min("last_known_humidity_minus_cumulative_humidity").over(w8).alias("running_min_last_known_humidity_minus_cumulative_humidity"),
    )
)
