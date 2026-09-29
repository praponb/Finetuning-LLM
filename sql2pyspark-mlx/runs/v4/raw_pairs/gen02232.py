from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("status_bits", "id")
w4 = Window.orderBy("status_bits", "id").rowsBetween(-3, -1)
w5 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w6 = Window.partitionBy("device_code").orderBy("pressure", "id")
w7 = Window.partitionBy("device_code").orderBy("reading_ts_day", "id")

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.concat_ws(",", "firmware", "device_code").alias("firmware_device_code_key"),
        F.dayofmonth("reading_ts").alias("reading_ts_day"),
    )
)

gaps = (
    filtered
    .withColumn("new_group_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id", F.sum("new_group_flag").over(w2))

metrics = (
    sessionized
    .select(
        "*",
        F.row_number().over(w3).alias("event_seq"),
        F.avg("status_bits").over(w4).alias("status_bits_avg_3_prior"),
        F.count("temp_c").over(w5).alias("temp_c_count_15m"),
        F.ntile(4).over(w6).alias("pressure_bucket"),
        F.row_number().over(w7).alias("seq_no"),
    )
)

result = (
    metrics
    .select("id", "device_code", "pressure", "temp_c", "humidity")
    .unpivot(
        ["id", "device_code"],
        ["pressure", "temp_c", "humidity"],
        "measure",
        "metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
