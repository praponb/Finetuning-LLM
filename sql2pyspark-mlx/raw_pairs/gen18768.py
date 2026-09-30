from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("reading_ts"), "device_code").orderBy("reading_ts", "id")
w_recent = Window.partitionBy("device_code").orderBy(F.col("humidity").desc(), "id")
w3 = Window.orderBy("reading_ts", "id")

valid = (
    df_device_readings
    .filter(F.col("firmware") != "1.0.3")
    .withColumn("temp_c_minus_pressure", (F.col("temp_c") - F.col("pressure")))
)

with_windows = (
    valid
    .filter(F.col("temp_c") >= 24)
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "pressure",
        F.lag("status_bits", 1).over(w).alias("prev_status_bits"),
        F.row_number().over(w_recent).alias("event_seq"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 01:00:00").cast("timestamp"))
    .withColumn("temp_c_int", F.col("temp_c").cast("int"))
)

with_windows_device_readings = (
    cleaned
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "pressure",
        "temp_c_int",
        F.lag("prev_status_bits").over(w3).alias("prev_prev_status_bits"),
        F.lead("event_seq").over(w3).alias("next_event_seq"),
        F.lag("event_seq").over(w3).alias("prev_event_seq"),
        F.row_number().over(w3).alias("event_seq_2"),
    )
)

result = (
    with_windows_device_readings
    .groupBy(
        F.date_trunc("day", "reading_ts").alias("activity_day"),
        F.date_trunc("hour", "reading_ts").alias("hour_window"),
    )
    .agg(
        F.min("reading_ts").alias("first_seen"),
        F.max("temp_c_int").alias("max_temp_c_int"),
        F.sum("pressure").alias("total_pressure"),
    )
    .filter(F.col("total_pressure") > 0)
)
