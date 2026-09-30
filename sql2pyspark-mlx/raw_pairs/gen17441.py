from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("humidity", "id")
w3 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy("pressure", "id")
w6 = Window.partitionBy(F.to_date("reading_ts")).orderBy("status_bits", "id").rowsBetween(-3, -1)
w7 = Window.orderBy("humidity_filled", "id").rowsBetween(-3, Window.currentRow)
w8 = Window.orderBy("temp_c_sum_1h", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("status_bits_max_15m", "id").rowsBetween(-6, -1)
w10 = Window.orderBy(F.col("temp_c_sum_1h").desc(), "id")
w11 = Window.orderBy(F.col("status_bits_avg_6_prior").desc(), "id")
w12 = Window.orderBy(F.col("humidity").asc_nulls_last())
w13 = Window.orderBy("next_temp_c_times_running_pressure", "id")

metrics = (
    df_device_readings
    .select(
        "id",
        "reading_ts",
        "humidity",
        "status_bits",
        F.sum("pressure").over(w_running).alias("running_pressure"),
        F.lead("temp_c", 1).over(w2).alias("next_temp_c"),
        F.max("status_bits").over(w3).alias("status_bits_max_15m"),
        F.sum("temp_c").over(w4).alias("temp_c_sum_1h"),
        F.lag("humidity", 2).over(w5).alias("prev_humidity"),
    )
)

cleaned = (
    metrics
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 08:00:00").cast("timestamp"))
    .select(
        "*",
        F.coalesce("humidity", F.lit(0)).alias("humidity_filled"),
        (F.col("running_pressure") / F.nullif("status_bits_max_15m", F.lit(0))).alias("running_pressure_per_status_bits_max_15m"),
    )
)

scored = (
    cleaned
    .filter(F.col("humidity").between(54, 75))
    .withColumn("prev_humidity_avg_3_prior", F.avg("prev_humidity").over(w6))
    .withColumn("status_bits_max_15m_avg_3", F.avg("status_bits_max_15m").over(w7))
    .withColumn("running_prev_humidity", F.sum("prev_humidity").over(w8))
    .withColumn("status_bits_avg_6_prior", F.avg("status_bits").over(w9))
    .withColumn("temp_c_sum_1h_bucket", F.ntile(5).over(w10))
)

cleaned_device_readings = (
    scored
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "humidity",
        "status_bits",
        "running_pressure",
        "next_temp_c",
        "temp_c_sum_1h",
        "humidity_filled",
        "status_bits_max_15m_avg_3",
        "running_prev_humidity",
        "status_bits_avg_6_prior",
        F.coalesce("running_pressure_per_status_bits_max_15m", F.lit(0)).alias("running_pressure_per_status_bits_max_15m_filled"),
        (F.col("status_bits_max_15m") * F.col("temp_c_sum_1h")).alias("status_bits_max_15m_times_temp_c_sum_1h"),
    )
)

filtered = (
    cleaned_device_readings
    .filter(F.col("status_bits_max_15m_avg_3") < 3)
    .select(
        "id",
        "humidity",
        "status_bits",
        "running_pressure",
        "next_temp_c",
        "temp_c_sum_1h",
        "running_prev_humidity",
        "status_bits_avg_6_prior",
        "running_pressure_per_status_bits_max_15m_filled",
        "status_bits_max_15m_times_temp_c_sum_1h",
        (F.col("next_temp_c") * F.col("running_pressure")).alias("next_temp_c_times_running_pressure"),
    )
)

metrics_device_readings = (
    filtered
    .withColumn("event_seq", F.row_number().over(w11))
    .withColumn("humidity_rank", F.rank().over(w12))
    .withColumn("next_status_bits_avg_6_prior", F.lead("status_bits_avg_6_prior", 1).over(w13))
)

result = (
    metrics_device_readings
    .select(
        "id",
        "humidity",
        "running_pressure",
        "next_temp_c",
        "running_prev_humidity",
        "status_bits_avg_6_prior",
        "next_temp_c_times_running_pressure",
        "event_seq",
        "humidity_rank",
    )
    .orderBy("running_prev_humidity")
)
