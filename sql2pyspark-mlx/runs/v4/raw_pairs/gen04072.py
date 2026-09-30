from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("days_since_reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")
w_grp_2 = Window.partitionBy("device_code").orderBy(F.col("pressure").desc_nulls_first())
w6 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_running = Window.partitionBy("device_code").orderBy("is_new_burst_3", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("device_code").orderBy(F.col("running_temp_c").desc_nulls_first(), "id")

filtered = (
    df_device_readings
    .filter(F.col("humidity") > 35)
    .select(
        "*",
        F.upper("device_code").alias("device_code_upper"),
        F.datediff(F.current_date(), "reading_ts").alias("days_since_reading_ts"),
    )
)

gaps = (
    filtered
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

with_gaps = (
    sessionized
    .withColumn("is_new_burst_2", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

sessions_device_readings = with_gaps.withColumn("island_id", F.sum("is_new_burst_2").over(w2))

with_gaps_2 = (
    sessions_device_readings
    .withColumn("new_group_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessions_device_readings_2 = with_gaps_2.withColumn("burst_id_2", F.sum("new_group_flag").over(w2))

gaps_2 = (
    sessions_device_readings_2
    .withColumn("is_new_burst_3", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized_device_readings = gaps_2.withColumn("burst_id_3", F.sum("is_new_burst_3").over(w2))

with_windows = (
    sessionized_device_readings
    .select(
        "*",
        F.last("temp_c", ignorenulls=True).over(w3).alias("last_known_temp_c"),
        F.row_number().over(w_grp).alias("seq_no"),
        F.percent_rank().over(w_grp_2).alias("pressure_percent_rank"),
        F.sum("burst_id").over(w6).alias("burst_id_sum_2h"),
        F.sum("temp_c").over(w_running).alias("running_temp_c"),
    )
)

latest = (
    with_windows
    .withColumn("row_num", F.row_number().over(w8))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .groupBy(
        F.when(F.col("humidity") >= 73, "PREMIUM").otherwise("STANDARD").alias("humidity_tier"),
        "island_id",
    )
    .agg(
        F.percentile_approx("pressure", 0.5).alias("pressure_p50"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
        F.percentile_approx("burst_id_3", [0.25, 0.5, 0.75]).alias("burst_id_3_quartiles"),
        F.max(F.when(F.col("device_code") == "DV-01", F.col("days_since_reading_ts"))).alias("dv_01_max_days_since_reading_ts"),
    )
    .orderBy(F.col("humidity_tier").desc())
)
