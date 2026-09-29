from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("humidity", "id")
w2 = Window.orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("temp_c").desc(), "id")
w_ordered = Window.orderBy("cumulative_status_bits_scaled_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy(F.col("cumulative_status_bits_scaled").asc_nulls_first())

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") > F.to_date(F.lit("2025-09-01")))
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        F.upper("device_code").alias("device_code_upper"),
    )
)

metrics = (
    filtered
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "status_bits",
        "device_code_upper",
        F.lead("temp_c", 1).over(w).alias("next_temp_c"),
        F.sum("status_bits").over(w2).alias("cumulative_status_bits"),
        F.ntile(4).over(w3).alias("temp_c_bucket"),
    )
)

filtered_device_readings = (
    metrics
    .filter(F.col("cumulative_status_bits").between(33, 38))
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "next_temp_c",
        "cumulative_status_bits",
        (F.col("temp_c") / F.nullif("status_bits", F.lit(0))).alias("temp_c_per_status_bits"),
    )
)

valid = (
    filtered_device_readings
    .filter((F.col("reading_ts") > F.to_timestamp(F.lit("2025-09-01 02:00:00"))) & (F.col("cumulative_status_bits") == 16))
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "cumulative_status_bits",
        "temp_c_per_status_bits",
        F.round((F.col("cumulative_status_bits") * 1.07), 2).alias("adjusted_cumulative_status_bits"),
        F.round((F.col("cumulative_status_bits") * 100), 2).alias("cumulative_status_bits_scaled"),
    )
)

base = (
    valid
    .filter((F.col("reading_ts") >= F.to_timestamp(F.lit("2025-09-01 02:00:00"))) & (F.col("adjusted_cumulative_status_bits") >= 29))
    .withColumn("cumulative_status_bits_scaled_times_temp_c_per_status_bits", (F.col("cumulative_status_bits_scaled") * F.col("temp_c_per_status_bits")))
    .withColumn("cumulative_status_bits_scaled_filled", F.coalesce("cumulative_status_bits_scaled", F.lit(0)))
)

valid_device_readings = (
    base
    .filter((F.col("reading_ts") >= F.lit("2025-09-01").cast("date")) & (F.col("cumulative_status_bits") != 59))
    .select(
        "*",
        (F.datediff(F.current_date(), "reading_ts") / 7).alias("weeks_since_reading_ts"),
        F.col("reading_ts").cast("date").alias("reading_ts_day"),
    )
)

valid_device_readings_2 = (
    valid_device_readings
    .filter(F.col("reading_ts_day") < "2024-11-23")
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "cumulative_status_bits_scaled",
        "cumulative_status_bits_scaled_times_temp_c_per_status_bits",
        "cumulative_status_bits_scaled_filled",
        "weeks_since_reading_ts",
        "reading_ts_day",
        F.date_trunc("year", "reading_ts_day").alias("reading_ts_day_period"),
        (F.col("cumulative_status_bits") + F.col("temp_c")).alias("cumulative_status_bits_plus_temp_c"),
    )
)

result = (
    valid_device_readings_2
    .filter(F.col("reading_ts_day_period") >= "2024-10-01")
    .select(
        "id",
        "cumulative_status_bits_scaled_times_temp_c_per_status_bits",
        "reading_ts_day",
        "cumulative_status_bits_plus_temp_c",
        F.avg("temp_c").over(w_ordered).alias("running_avg_temp_c"),
        F.sum("temp_c").over(w_ordered).alias("running_sum_temp_c"),
        F.count("temp_c").over(w_ordered).alias("running_count_temp_c"),
        F.min("temp_c").over(w_ordered).alias("running_min_temp_c"),
        F.rank().over(w_part).alias("cumulative_status_bits_scaled_rank"),
    )
)
