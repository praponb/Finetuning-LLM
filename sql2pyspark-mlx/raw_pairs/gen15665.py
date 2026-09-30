from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("temp_c").asc_nulls_last())
w_part = Window.orderBy("pressure_avg_2h", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.orderBy("temp_c_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("device_code").orderBy(F.col("running_status_bits").desc(), "id")
w7 = Window.partitionBy("device_code")
w8 = Window.orderBy(F.col("pressure_avg_2").desc())
w9 = Window.orderBy("temp_c_cume_dist_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy(F.col("device_code_min_status_bits").asc_nulls_last())
w11 = Window.orderBy("cumulative_temp_c", "id").rowsBetween(-5, -1)

cleaned = (
    df_device_readings
    .filter(F.to_date("reading_ts") >= "2024-12-01")
    .select(
        "*",
        F.date_format("reading_ts", "yyyy-MM").alias("reading_ts_label"),
        F.when(F.col("temp_c") >= 37.8, "High").when(F.col("temp_c") >= 22.4, "Medium").otherwise("Low").alias("temp_c_tier"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "device_code",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        "reading_ts_label",
        F.avg("pressure").over(w).alias("pressure_avg_2h"),
        F.sum("status_bits").over(w2).alias("running_status_bits"),
        F.cume_dist().over(w3).alias("temp_c_cume_dist"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "firmware",
        "reading_ts_label",
        "temp_c_cume_dist",
        F.avg("pressure").over(w_part).alias("pressure_avg_2"),
        F.sum("temp_c").over(w5).alias("cumulative_temp_c"),
        F.ntile(4).over(w_hist).alias("running_status_bits_bucket"),
        F.min("status_bits").over(w7).alias("device_code_min_status_bits"),
    )
)

cleaned_device_readings = (
    enriched
    .filter(F.col("temp_c_cume_dist").isNotNull())
    .select(
        "id",
        "firmware",
        "reading_ts_label",
        "pressure_avg_2",
        "cumulative_temp_c",
        "running_status_bits_bucket",
        "device_code_min_status_bits",
        F.col("temp_c_cume_dist").cast("int").alias("temp_c_cume_dist_int"),
    )
)

metrics = (
    cleaned_device_readings
    .select(
        "*",
        F.percent_rank().over(w8).alias("pressure_avg_2_percent_rank"),
        F.sum("cumulative_temp_c").over(w9).alias("running_cumulative_temp_c"),
        F.rank().over(w10).alias("device_code_min_status_bits_rank"),
        F.sum("temp_c_cume_dist_int").over(w9).alias("running_temp_c_cume_dist_int"),
        F.avg("pressure_avg_2").over(w11).alias("pressure_avg_2_avg_5_prior"),
    )
)

remaining_keys = (
    metrics
    .select("id", "firmware")
    .exceptAll(
        df_device_readings
        .filter(F.col("pressure").between(1012, 1020))
        .select("id", "firmware")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.firmware") == F.col("k.firmware")), "inner")
    .select("s.*")
)

grouped = (
    reconciled
    .groupBy(
        F.when(F.col("running_status_bits_bucket") >= 9, "HIGH").otherwise("LOW").alias("running_status_bits_bucket_tier"),
    )
    .agg(
        F.percentile_approx("cumulative_temp_c", [0.25, 0.5, 0.75]).alias("cumulative_temp_c_quartiles"),
        F.percentile_approx("pressure_avg_2_percent_rank", [0.25, 0.5, 0.75]).alias("pressure_avg_2_percent_rank_quartiles"),
        F.round(F.avg("device_code_min_status_bits_rank"), 2).alias("avg_device_code_min_status_bits_rank"),
        F.countDistinct("firmware").alias("distinct_firmwares"),
        F.countDistinct("firmware").alias("distinct_firmwares_2"),
        F.round(F.avg("pressure_avg_2_avg_5_prior"), 2).alias("avg_pressure_avg_2_avg_5_prior"),
    )
)

result = (
    grouped
    .select(
        "running_status_bits_bucket_tier",
        "cumulative_temp_c_quartiles",
        "pressure_avg_2_percent_rank_quartiles",
        "distinct_firmwares_2",
    )
)
