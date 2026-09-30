from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("device_code").orderBy("humidity", "id")
w_recent = Window.partitionBy(F.year("reading_ts"), "device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("prev_status_bits").desc_nulls_last())
w4 = Window.partitionBy("device_code").orderBy("next_status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("adjusted_prev_status_bits", "id").rowsBetween(-2, Window.currentRow)
w6 = Window.orderBy("running_max_pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("next_status_bits").desc(), "id")

valid = (
    df_device_readings
    .filter(F.col("reading_ts") > F.to_timestamp(F.lit("2025-09-01 06:00:00")))
    .withColumn("pressure_int", F.col("pressure").cast("int"))
    .withColumn("days_since_reading_ts", F.datediff(F.current_date(), "reading_ts"))
)

scored = (
    valid
    .select(
        "*",
        F.lag("humidity").over(w_part).alias("prev_humidity"),
        F.lead("humidity").over(w_part).alias("next_humidity"),
        F.lead("status_bits").over(w_part).alias("next_status_bits"),
        F.lag("status_bits").over(w_part).alias("prev_status_bits"),
        F.row_number().over(w_part).alias("seq_no"),
        F.sum("pressure").over(w_recent).alias("running_pressure"),
    )
)

ranked = (
    scored
    .withColumn("rank_in_group", F.rank().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "pressure_int",
        "next_status_bits",
        "prev_status_bits",
        "seq_no",
    )
)

base = (
    ranked
    .filter(F.col("reading_ts") > F.lit("2025-09-01 04:00:00").cast("timestamp"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "firmware",
        "pressure_int",
        "next_status_bits",
        "prev_status_bits",
        "seq_no",
        F.date_format("reading_ts", "yyyy-MM-dd").alias("reading_ts_label"),
    )
)

filtered = (
    base
    .filter(F.col("reading_ts") > F.to_timestamp(F.lit("2025-09-01 05:00:00")))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "pressure",
        "next_status_bits",
        "seq_no",
        "reading_ts_label",
        F.round((F.col("prev_status_bits") * 1.15), 2).alias("adjusted_prev_status_bits"),
    )
)

with_windows = (
    filtered
    .select(
        "id",
        "next_status_bits",
        "reading_ts_label",
        "adjusted_prev_status_bits",
        F.avg("pressure").over(w4).alias("running_avg_pressure"),
        F.min("pressure").over(w4).alias("running_min_pressure"),
        F.max("pressure").over(w4).alias("running_max_pressure"),
    )
)

result = (
    with_windows
    .withColumn("running_max_pressure_avg_2", F.avg("running_max_pressure").over(w5))
    .withColumn("cumulative_running_avg_pressure", F.sum("running_avg_pressure").over(w6))
    .withColumn("event_seq", F.row_number().over(w7))
)
