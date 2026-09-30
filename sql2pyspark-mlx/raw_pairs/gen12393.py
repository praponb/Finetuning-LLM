from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("pressure", "id")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w4 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy("seq_no", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy("device_code").orderBy(F.col("seq_no").desc(), "id")
w_hist = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(-3, Window.currentRow)
w_ordered = Window.partitionBy("device_code").orderBy("last4_avg_seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w10 = Window.partitionBy("device_code").orderBy("last4_avg_seq_no", "id")
w11 = Window.partitionBy("device_code")

metrics = (
    df_device_readings
    .filter(F.col("status_bits") > 10)
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

with_gaps = (
    metrics
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w3).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w3) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "seq_no",
        F.sum("session_start_flag").over(w4).alias("session_id"),
    )
)

with_windows = (
    islands
    .select(
        "*",
        F.sum("seq_no").over(w5).alias("last4_sum_seq_no"),
        F.min("seq_no").over(w5).alias("last4_min_seq_no"),
        F.avg("seq_no").over(w5).alias("last4_avg_seq_no"),
        F.ntile(10).over(w6).alias("seq_no_bucket"),
    )
)

with_windows_device_readings = (
    with_windows
    .withColumn("last4_count_last4_min_seq_no", F.count("last4_min_seq_no").over(w_hist))
    .withColumn("last4_min_last4_min_seq_no", F.min("last4_min_seq_no").over(w_hist))
    .withColumn("last4_max_last4_min_seq_no", F.max("last4_min_seq_no").over(w_hist))
    .withColumn("cumulative_status_bits", F.sum("status_bits").over(w_ordered))
)

with_windows_device_readings_2 = (
    with_windows_device_readings
    .select(
        "id",
        "device_code",
        "humidity",
        "pressure",
        "status_bits",
        "session_id",
        "last4_min_seq_no",
        "last4_avg_seq_no",
        "last4_count_last4_min_seq_no",
        "last4_max_last4_min_seq_no",
        "cumulative_status_bits",
        F.sum("pressure").over(w9).alias("pressure_sum_2h"),
        F.row_number().over(w10).alias("row_seq"),
        F.sum("pressure").over(w11).alias("device_code_sum_pressure"),
    )
)

result = (
    with_windows_device_readings_2
    .select("id", "device_code", "pressure", "last4_max_last4_min_seq_no")
    .unpivot(
        ids=["id", "device_code"],
        values=["pressure", "last4_max_last4_min_seq_no"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
