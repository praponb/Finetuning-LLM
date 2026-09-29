from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("temp_c", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("device_code").orderBy("new_group_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("device_code").orderBy("running_temp_c", "id").rowsBetween(-2, -1)
w7 = Window.orderBy("temp_c", "id")

deduped = (
    df_device_readings
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

prev_events = deduped.withColumn("prev_reading_ts", F.lag("reading_ts").over(w2))

gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = gaps.withColumn("burst_id", F.sum("new_group_flag").over(w3))

with_windows = (
    sessions_device_readings
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.sum("temp_c").over(w4).alias("running_temp_c"),
    )
)

enriched = (
    with_windows
    .withColumn("cumulative_humidity", F.sum("humidity").over(w_recent))
    .withColumn("burst_id_avg_2_prior", F.avg("burst_id").over(w6))
    .withColumn("prev_row_seq", F.lag("row_seq", 1).over(w7))
    .withColumn("running_new_group_flag", F.sum("new_group_flag").over(w3))
)

cleaned = (
    enriched
    .filter(F.col("new_group_flag").between(0, 1))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        "burst_id",
        "row_seq",
        "cumulative_humidity",
        "burst_id_avg_2_prior",
        "prev_row_seq",
        "running_new_group_flag",
        F.when(F.col("prev_row_seq") >= 8, "High").when(F.col("prev_row_seq") >= 4, "Medium").otherwise("Low").alias("prev_row_seq_level"),
    )
)

filtered = (
    cleaned
    .filter((F.col("reading_ts") >= F.lit("2025-09-01").cast("date")) & F.col("device_code").isin(["DV-02", "DV-01", "DV-04"]))
    .select("*", F.col("reading_ts").cast("date").alias("reading_ts_day"))
)

result = (
    filtered
    .select(
        "id",
        "device_code",
        "reading_ts",
        "firmware",
        "burst_id",
        "row_seq",
        "burst_id_avg_2_prior",
        "prev_row_seq",
        "prev_row_seq_level",
    )
)
