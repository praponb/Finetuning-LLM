from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("reading_ts"), "device_code").orderBy("years_since_reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.to_date("reading_ts")).orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.year("reading_ts")).orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_ordered = Window.partitionBy(F.date_trunc("MONTH", "reading_ts")).orderBy(F.col("reading_ts").desc(), "id")
w5 = Window.partitionBy("device_code").orderBy(F.col("cumulative_temp_c").desc_nulls_last(), "id")
w6 = Window.partitionBy("device_code").orderBy("pressure", "id")
w7 = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("device_code").orderBy("years_since_reading_ts_count_2h", "id")
w_hist_2 = Window.partitionBy(F.date_trunc("MONTH", "reading_ts"), "device_code").orderBy("seq_no_per_row_num", "id")

filtered = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01").cast("date")) & (F.col("device_code") == "DV-03"))
    .select(
        "*",
        (F.datediff(F.current_date(), "reading_ts") / 365).alias("years_since_reading_ts"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.last("years_since_reading_ts", ignorenulls=True).over(w).alias("last_known_years_since_reading_ts"),
        F.sum("temp_c").over(w2).alias("cumulative_temp_c"),
        F.count("years_since_reading_ts").over(w3).alias("years_since_reading_ts_count_2h"),
        F.row_number().over(w_ordered).alias("row_seq"),
    )
)

latest = (
    metrics
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

metrics_device_readings = (
    latest
    .withColumn("seq_no", F.row_number().over(w6))
    .withColumn("cumulative_pressure", F.sum("pressure").over(w7))
)

cleaned = (
    metrics_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "years_since_reading_ts",
        "last_known_years_since_reading_ts",
        "cumulative_temp_c",
        "years_since_reading_ts_count_2h",
        "row_seq",
        "row_num",
        "seq_no",
        "cumulative_pressure",
        F.concat_ws(",", "device_code", "firmware").alias("device_code_firmware_key"),
    )
)

valid = (
    cleaned
    .filter(F.col("row_num") != 1)
    .withColumn("seq_no_per_row_num", F.round((F.col("seq_no") / F.col("row_num")), 2))
)

scored = (
    valid
    .withColumn("running_status_bits", F.sum("status_bits").over(w8))
    .withColumn("years_since_reading_ts_count_2h_bucket", F.ntile(5).over(w_hist))
    .withColumn("seq_no_per_row_num_bucket", F.ntile(3).over(w_hist_2))
)

result = (
    scored
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "cumulative_temp_c",
        "years_since_reading_ts_count_2h",
    )
    .orderBy("firmware")
)
