from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("pressure", "id")
w2 = Window.partitionBy("device_code").orderBy("humidity", "id")
w3 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("device_code").orderBy("running_count_status_bits", "id").rowsBetween(-6, Window.currentRow)
w_grp = Window.orderBy("running_count_status_bits_plus_pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("last7_avg_event_seq_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 04:00:00").cast("timestamp"))
    .withColumn("least_humidity_temp_c", F.least("humidity", "temp_c"))
)

cleaned = (
    base
    .filter(F.col("reading_ts") > F.lit("2025-09-01 06:00:00").cast("timestamp"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        F.when(F.col("temp_c") >= 33.4, "High").when(F.col("temp_c") >= 25.7, "Medium").otherwise("Low").alias("temp_c_tier"),
    )
)

scored = (
    cleaned
    .withColumn("next_status_bits", F.lead("status_bits").over(w))
    .withColumn("prev_humidity", F.lag("humidity").over(w))
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("event_seq_2", F.row_number().over(w2))
)

with_windows = (
    scored
    .filter(F.col("event_seq") > 4)
    .select(
        "id",
        "device_code",
        "pressure",
        "status_bits",
        "temp_c_tier",
        "event_seq_2",
        F.avg("status_bits").over(w3).alias("running_avg_status_bits"),
        F.count("status_bits").over(w3).alias("running_count_status_bits"),
        F.min("status_bits").over(w3).alias("running_min_status_bits"),
    )
)

cleaned_device_readings = (
    with_windows
    .filter(F.col("status_bits") < 4)
    .withColumn("running_count_status_bits_plus_pressure", (F.col("running_count_status_bits") + F.col("pressure")))
)

with_windows_device_readings = (
    cleaned_device_readings
    .select(
        "id",
        "temp_c_tier",
        "running_count_status_bits",
        "running_count_status_bits_plus_pressure",
        F.avg("event_seq_2").over(w_recent).alias("last7_avg_event_seq_2"),
        F.count("event_seq_2").over(w_recent).alias("last7_count_event_seq_2"),
        F.sum("event_seq_2").over(w_recent).alias("last7_sum_event_seq_2"),
    )
)

scored_device_readings = (
    with_windows_device_readings
    .select(
        "id",
        "running_count_status_bits",
        "last7_avg_event_seq_2",
        F.max("last7_sum_event_seq_2").over(w_grp).alias("running_max_last7_sum_event_seq_2"),
        F.avg("last7_sum_event_seq_2").over(w_grp).alias("running_avg_last7_sum_event_seq_2"),
        F.sum("last7_sum_event_seq_2").over(w_grp).alias("running_sum_last7_sum_event_seq_2"),
        F.sum("last7_count_event_seq_2").over(w6).alias("cumulative_last7_count_event_seq_2"),
    )
)

result = (
    scored_device_readings
    .groupBy(
        F.when(F.col("cumulative_last7_count_event_seq_2") >= 88, "PREMIUM").otherwise("STANDARD").alias("cumulative_last7_count_event_seq_2_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("running_avg_last7_sum_event_seq_2", 0.9).alias("running_avg_last7_sum_event_seq_2_p90"),
        F.percentile_approx("running_max_last7_sum_event_seq_2", [0.25, 0.5, 0.75]).alias("running_max_last7_sum_event_seq_2_quartiles"),
        F.percentile_approx("last7_avg_event_seq_2", 0.95).alias("last7_avg_event_seq_2_p95"),
    )
    .filter(F.col("last7_avg_event_seq_2_p95") >= 0)
    .orderBy(F.col("last7_avg_event_seq_2_p95").desc_nulls_last())
)
