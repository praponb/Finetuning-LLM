from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("temp_c").asc_nulls_last())
w2 = Window.partitionBy("device_code").orderBy(F.col("temp_c_whole").desc())
w3 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("device_code")
w5 = Window.partitionBy("device_code").orderBy("humidity", "id")
w6 = Window.partitionBy("device_code").orderBy("next_pressure", "id")
w7 = Window.orderBy("next_pressure", "id")
w8 = Window.orderBy("temp_c_avg_5_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_device_readings
    .filter(F.col("status_bits") < 10)
    .select("*", F.floor("temp_c").alias("temp_c_whole"))
)

metrics = (
    valid
    .select(
        "id",
        "device_code",
        "pressure",
        "firmware",
        "temp_c_whole",
        F.cume_dist().over(w).alias("temp_c_cume_dist"),
        F.percent_rank().over(w2).alias("temp_c_whole_percent_rank"),
        F.avg("temp_c").over(w3).alias("temp_c_avg_5_prior"),
        F.min("pressure").over(w4).alias("device_code_min_pressure"),
        F.lead("pressure", 2).over(w5).alias("next_pressure"),
    )
)

filtered = (
    metrics
    .filter(F.col("temp_c_whole_percent_rank") <= 0.69)
    .select(
        "*",
        (F.col("device_code_min_pressure") * F.col("temp_c_whole")).alias("device_code_min_pressure_times_temp_c_whole"),
        F.upper("firmware").alias("firmware_upper"),
    )
)

ranked = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "pressure",
        "temp_c_cume_dist",
        "temp_c_avg_5_prior",
        "device_code_min_pressure",
        "next_pressure",
        "device_code_min_pressure_times_temp_c_whole",
    )
)

cleaned = (
    ranked
    .filter(F.col("temp_c_cume_dist") <= 0.24)
    .select(
        "id",
        "pressure",
        "temp_c_avg_5_prior",
        "device_code_min_pressure",
        "next_pressure",
        "device_code_min_pressure_times_temp_c_whole",
        F.col("device_code_min_pressure").cast("double").alias("device_code_min_pressure_num"),
        F.round((F.col("device_code_min_pressure_times_temp_c_whole") / F.col("device_code_min_pressure")), 2).alias("device_code_min_pressure_times_temp_c_whole_per_device_code_min_pressure"),
    )
)

metrics_device_readings = (
    cleaned
    .select(
        "id",
        "temp_c_avg_5_prior",
        "next_pressure",
        "device_code_min_pressure_times_temp_c_whole",
        "device_code_min_pressure_times_temp_c_whole_per_device_code_min_pressure",
        F.lag("next_pressure").over(w7).alias("prev_next_pressure"),
        F.lead("next_pressure").over(w7).alias("next_next_pressure"),
        F.lead("device_code_min_pressure_times_temp_c_whole_per_device_code_min_pressure").over(w7).alias("next_device_code_min_pressure_times_temp_c_whole_per_device_code_min_pressure"),
        F.lag("device_code_min_pressure_times_temp_c_whole_per_device_code_min_pressure").over(w7).alias("prev_device_code_min_pressure_times_temp_c_whole_per_device_code_min_pressure"),
        F.row_number().over(w7).alias("seq_no"),
        F.first("device_code_min_pressure", ignorenulls=True).over(w8).alias("first_known_device_code_min_pressure"),
    )
)

result = (
    metrics_device_readings
    .groupBy("seq_no")
    .agg(
        F.max("first_known_device_code_min_pressure").alias("max_first_known_device_code_min_pressure"),
        F.percentile_approx("device_code_min_pressure_times_temp_c_whole_per_device_code_min_pressure", 0.5).alias("device_code_min_pressure_times_temp_c_whole_per_device_code_min_pressure_p50"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("temp_c_avg_5_prior"), 2).alias("avg_temp_c_avg_5_prior"),
    )
)
