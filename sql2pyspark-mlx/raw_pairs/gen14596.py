from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("device_code").orderBy("pressure", "id")
w_ordered = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(-6, -1)
w5 = Window.orderBy("next_pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy(F.col("status_bits_avg_6").desc())
w7 = Window.orderBy("pressure_bucket", "id")
w8 = Window.orderBy("reading_ts_day", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01 04:00:00").cast("timestamp")) & (F.col("status_bits") <= 11))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "pressure",
        "status_bits",
        F.col("reading_ts").cast("date").alias("reading_ts_day"),
    )
)

enriched = (
    cleaned
    .filter(F.col("device_code").isin(["DV-02", "DV-04", "DV-01"]))
    .select(
        "id",
        "reading_ts",
        "pressure",
        "status_bits",
        "reading_ts_day",
        F.ntile(3).over(w_recent).alias("pressure_bucket"),
        F.lead("pressure", 1).over(w_recent).alias("next_pressure"),
        F.avg("status_bits").over(w_ordered).alias("status_bits_avg_6"),
        F.first("pressure", ignorenulls=True).over(w3).alias("first_known_pressure"),
        F.avg("pressure").over(w4).alias("pressure_avg_6_prior"),
    )
)

valid = (
    enriched
    .filter(F.col("next_pressure") <= 1010)
    .select(
        "id",
        "reading_ts",
        "reading_ts_day",
        "pressure_bucket",
        "next_pressure",
        "status_bits_avg_6",
        F.when(F.col("status_bits") >= 12, "High").when(F.col("status_bits") >= 3, "Medium").otherwise("Low").alias("status_bits_band"),
    )
)

metrics = (
    valid
    .withColumn("running_avg_pressure_bucket", F.avg("pressure_bucket").over(w5))
    .withColumn("running_min_pressure_bucket", F.min("pressure_bucket").over(w5))
    .withColumn("running_max_pressure_bucket", F.max("pressure_bucket").over(w5))
)

enriched_device_readings = (
    metrics
    .select(
        "id",
        "reading_ts",
        "reading_ts_day",
        "pressure_bucket",
        "next_pressure",
        "status_bits_band",
        F.percent_rank().over(w_running).alias("status_bits_avg_6_percent_rank"),
        F.row_number().over(w7).alias("event_seq"),
    )
)

result = (
    enriched_device_readings
    .select(
        "id",
        "reading_ts",
        "reading_ts_day",
        F.sum("status_bits_avg_6_percent_rank").over(w8).alias("running_sum_status_bits_avg_6_percent_rank"),
        F.min("status_bits_avg_6_percent_rank").over(w8).alias("running_min_status_bits_avg_6_percent_rank"),
        F.max("status_bits_avg_6_percent_rank").over(w8).alias("running_max_status_bits_avg_6_percent_rank"),
        F.avg("status_bits_avg_6_percent_rank").over(w8).alias("running_avg_status_bits_avg_6_percent_rank"),
    )
)
