from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("humidity_rounded", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter((F.col("reading_ts") > F.to_date(F.lit("2025-09-01"))) & (F.to_date("reading_ts") < "2023-11-01"))
    .withColumn("temp_c_plus_status_bits", (F.col("temp_c") + F.col("status_bits")))
)

filtered_device_readings = (
    filtered
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "status_bits",
        "firmware",
        "temp_c_plus_status_bits",
        F.round("humidity", 0).alias("humidity_rounded"),
    )
)

metrics = (
    filtered_device_readings
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        F.lag("temp_c_plus_status_bits").over(w).alias("prev_temp_c_plus_status_bits"),
        F.lag("temp_c").over(w).alias("prev_temp_c"),
        F.row_number().over(w).alias("event_seq"),
    )
)

gaps = (
    metrics
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w2).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w2) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("session_start_flag").over(w3))

with_gaps = (
    sessionized
    .withColumn("session_start_flag_2", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w2) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("session_start_flag_2").over(w3))

result = (
    islands
    .groupBy(
        F.date_trunc("DAY", "reading_ts").alias("activity_day"),
        F.when(F.col("session_start_flag_2") >= 1, "HIGH").otherwise("LOW").alias("session_start_flag_2_tier"),
    )
    .agg(
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.min("reading_ts").alias("first_reading_ts"),
        F.percentile_approx("temp_c", 0.5).alias("temp_c_p50"),
        F.sum(F.when(F.col("device_code") == "DV-02", F.col("session_start_flag")).otherwise(0)).alias("dv_02_session_start_flag"),
        F.round(F.avg("prev_temp_c"), 2).alias("avg_prev_temp_c"),
    )
    .filter(F.col("num_device_codes") < 1000)
)
