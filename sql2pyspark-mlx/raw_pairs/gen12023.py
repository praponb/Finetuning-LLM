from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("pressure").desc(), "id")
w4 = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")
w5 = Window.partitionBy("device_code").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.month("reading_ts"), "device_code")
w7 = Window.partitionBy("device_code").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("corr_temp_c_status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("temp_c", "id")
w10 = Window.partitionBy(F.date_trunc("week", "reading_ts")).orderBy("last_known_row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w12 = Window.orderBy("running_first_known_pressure", "id")
w13 = Window.orderBy("session_no_sum_1h")

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01 05:00:00").cast("timestamp"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        F.round((F.col("pressure") / F.col("temp_c")), 2).alias("pressure_per_temp_c"),
    )
)

with_gaps = (
    cleaned
    .withColumn("new_group_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

metrics = (
    sessionized
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("seq_no", F.row_number().over(w4))
)

with_windows = (
    metrics
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "new_group_flag",
        "session_no",
        "row_seq",
        F.first("pressure", ignorenulls=True).over(w5).alias("first_known_pressure"),
        F.corr("temp_c", "status_bits").over(w6).alias("corr_temp_c_status_bits"),
        F.last("row_seq", ignorenulls=True).over(w7).alias("last_known_row_seq"),
    )
)

with_windows_device_readings = (
    with_windows
    .select(
        "id",
        "temp_c",
        "pressure",
        "new_group_flag",
        "session_no",
        "row_seq",
        "corr_temp_c_status_bits",
        F.sum("first_known_pressure").over(w8).alias("running_first_known_pressure"),
        F.ntile(10).over(w9).alias("temp_c_bucket"),
        F.last("new_group_flag", ignorenulls=True).over(w10).alias("last_known_new_group_flag"),
        F.sum("session_no").over(w11).alias("session_no_sum_1h"),
        F.corr("pressure", "temp_c").over(w6).alias("corr_pressure_temp_c"),
    )
)

metrics_device_readings = (
    with_windows_device_readings
    .select(
        "*",
        F.lead("last_known_new_group_flag", 1).over(w12).alias("next_last_known_new_group_flag"),
        F.rank().over(w13).alias("session_no_sum_1h_rank"),
    )
)

cleaned_device_readings = (
    metrics_device_readings
    .filter(F.col("next_last_known_new_group_flag").isNotNull())
    .select(
        "id",
        "pressure",
        "row_seq",
        "corr_temp_c_status_bits",
        "running_first_known_pressure",
        "temp_c_bucket",
        "last_known_new_group_flag",
        "session_no_sum_1h",
        "corr_pressure_temp_c",
        "next_last_known_new_group_flag",
        F.greatest("pressure", "new_group_flag").alias("greatest_pressure_new_group_flag"),
    )
)

totals = (
    cleaned_device_readings
    .groupBy(
        F.when(F.col("corr_temp_c_status_bits") >= 0.41, "HIGH").otherwise("LOW").alias("corr_temp_c_status_bits_tier"),
        F.when(F.col("next_last_known_new_group_flag") >= 1, "PREMIUM").otherwise("STANDARD").alias("next_last_known_new_group_flag_tier"),
    )
    .agg(
        F.percentile_approx("temp_c_bucket", [0.25, 0.5, 0.75]).alias("temp_c_bucket_quartiles"),
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    totals
    .select(
        "corr_temp_c_status_bits_tier",
        "next_last_known_new_group_flag_tier",
        "distinct_ids",
    )
    .orderBy(F.col("corr_temp_c_status_bits_tier").desc_nulls_first())
)
