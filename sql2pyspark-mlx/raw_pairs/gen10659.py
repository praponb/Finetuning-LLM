from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("WEEK", "reading_ts")).orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("temp_c", "id")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w4 = Window.orderBy("humidity_count_15m", "id").rowsBetween(-3, -1)
w5 = Window.partitionBy(F.year("reading_ts"), "device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w_grp = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(-2, Window.currentRow)
w7 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("status_bits").between(5, 9))
    .select(
        "*",
        (F.col("humidity") * F.col("status_bits")).alias("humidity_times_status_bits"),
    )
)

scored = (
    filtered
    .filter(F.col("status_bits") < 6)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "firmware",
        "humidity_times_status_bits",
        F.count("humidity").over(w).alias("humidity_count_15m"),
        F.ntile(3).over(w2).alias("temp_c_bucket"),
    )
)

deduped = (
    scored
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

scored_device_readings = (
    deduped
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity_times_status_bits",
        "humidity_count_15m",
        F.avg("humidity_count_15m").over(w4).alias("humidity_count_15m_avg_3_prior"),
        F.sum("temp_c_bucket").over(w5).alias("temp_c_bucket_sum_15m"),
    )
)

with_windows = (
    scored_device_readings
    .withColumn("last3_min_temp_c_bucket_sum_15m", F.min("temp_c_bucket_sum_15m").over(w_grp))
    .withColumn("last3_max_temp_c_bucket_sum_15m", F.max("temp_c_bucket_sum_15m").over(w_grp))
    .withColumn("last3_count_temp_c_bucket_sum_15m", F.count("temp_c_bucket_sum_15m").over(w_grp))
    .withColumn("last3_sum_temp_c_bucket_sum_15m", F.sum("temp_c_bucket_sum_15m").over(w_grp))
)

with_prev = with_windows.withColumn("prev_reading_ts", F.lag("reading_ts").over(w3))

with_gaps = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity_times_status_bits",
        "humidity_count_15m",
        "humidity_count_15m_avg_3_prior",
        "temp_c_bucket_sum_15m",
        "last3_min_temp_c_bucket_sum_15m",
        "last3_max_temp_c_bucket_sum_15m",
        "last3_count_temp_c_bucket_sum_15m",
        "last3_sum_temp_c_bucket_sum_15m",
        F.sum("new_group_flag").over(w7).alias("session_no"),
    )
)

with_gaps_2 = (
    islands
    .withColumn("is_new_session", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w3) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

sessionized = with_gaps_2.withColumn("session_id", F.sum("is_new_session").over(w7))

result = (
    sessionized
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity_times_status_bits",
        "humidity_count_15m_avg_3_prior",
        "last3_min_temp_c_bucket_sum_15m",
        "last3_count_temp_c_bucket_sum_15m",
        "last3_sum_temp_c_bucket_sum_15m",
        "session_no",
        F.when(F.col("device_code").rlike("^(DV-02|DV-01)$"), "EXTENDED").when(F.col("device_code").rlike("^(DV-03|DV-04)$"), "PRIORITY").otherwise("GROUP_A").alias("device_code_zone"),
    )
    .orderBy(F.col("id").desc())
)
