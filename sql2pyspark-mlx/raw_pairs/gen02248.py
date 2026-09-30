from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("pressure").asc_nulls_last())
w3 = Window.orderBy(F.col("humidity_int").desc(), "id")
w4 = Window.partitionBy(F.year("reading_ts")).orderBy("running_max_temp_c", "id")
w_grp = Window.orderBy("pressure", "id").rowsBetween(-2, -1)
w6 = Window.orderBy("running_avg_temp_c", "id")

valid = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01 08:00:00").cast("timestamp")) & (F.col("status_bits") >= 6))
    .select("*", F.round("humidity", 2).alias("humidity_rounded"))
)

valid_device_readings = (
    valid
    .filter(F.col("temp_c") < 32)
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        "humidity_rounded",
        F.col("humidity").cast("int").alias("humidity_int"),
    )
)

with_windows = (
    valid_device_readings
    .select(
        "*",
        F.max("temp_c").over(w).alias("running_max_temp_c"),
        F.avg("temp_c").over(w).alias("running_avg_temp_c"),
        F.min("temp_c").over(w).alias("running_min_temp_c"),
        F.cume_dist().over(w2).alias("pressure_cume_dist"),
    )
)

with_windows_device_readings = (
    with_windows
    .filter(F.col("humidity_int") != 46)
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "running_avg_temp_c",
        "running_min_temp_c",
        F.ntile(5).over(w3).alias("humidity_int_bucket"),
        F.ntile(10).over(w4).alias("running_max_temp_c_bucket"),
        F.avg("humidity_int").over(w_grp).alias("humidity_int_avg_2_prior"),
        F.ntile(3).over(w6).alias("running_avg_temp_c_bucket"),
    )
)

valid_device_readings_2 = (
    with_windows_device_readings
    .filter(F.hour("reading_ts").between(10, 17))
    .select(
        "id",
        "reading_ts",
        "running_avg_temp_c",
        "running_min_temp_c",
        "humidity_int_bucket",
        "humidity_int_avg_2_prior",
        "running_avg_temp_c_bucket",
        (F.col("running_min_temp_c") * F.col("running_avg_temp_c")).alias("running_min_temp_c_times_running_avg_temp_c"),
        (F.col("running_min_temp_c") + F.col("running_avg_temp_c")).alias("running_min_temp_c_plus_running_avg_temp_c"),
    )
)

base = (
    valid_device_readings_2
    .filter(F.col("humidity_int_bucket") < 3)
    .withColumn("months_since_reading_ts", (F.datediff(F.current_date(), "reading_ts") / 30))
)

result = (
    base
    .select(
        "id",
        "reading_ts",
        "running_min_temp_c",
        "humidity_int_avg_2_prior",
        "running_avg_temp_c_bucket",
        "running_min_temp_c_times_running_avg_temp_c",
        "running_min_temp_c_plus_running_avg_temp_c",
        "months_since_reading_ts",
    )
    .orderBy("humidity_int_avg_2_prior")
)
