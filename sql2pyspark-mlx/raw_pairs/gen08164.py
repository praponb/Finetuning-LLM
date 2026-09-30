from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("status_bits").desc(), "id")
w3 = Window.partitionBy(F.year("reading_ts"))
w_running = Window.orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("running_max_status_bits_bucket", "id")
w6 = Window.partitionBy(F.date_trunc("week", "reading_ts")).orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        (F.col("temp_c") - F.col("pressure")).alias("temp_c_minus_pressure"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.first("pressure", ignorenulls=True).over(w).alias("first_known_pressure"),
        F.ntile(4).over(w2).alias("status_bits_bucket"),
        F.corr("humidity", "pressure").over(w3).alias("corr_humidity_pressure"),
    )
)

valid = (
    enriched
    .filter(F.col("firmware") == "2.0.1")
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        "temp_c_minus_pressure",
        "first_known_pressure",
        "status_bits_bucket",
        "corr_humidity_pressure",
        F.ceil("pressure").alias("pressure_whole"),
        F.round((F.col("temp_c_minus_pressure") * 0.85), 2).alias("temp_c_minus_pressure_adjusted"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "reading_ts",
        "humidity",
        "pressure",
        "firmware",
        "corr_humidity_pressure",
        "pressure_whole",
        "temp_c_minus_pressure_adjusted",
        F.sum("status_bits_bucket").over(w_running).alias("running_sum_status_bits_bucket"),
        F.count("status_bits_bucket").over(w_running).alias("running_count_status_bits_bucket"),
        F.max("status_bits_bucket").over(w_running).alias("running_max_status_bits_bucket"),
    )
)

metrics_device_readings = (
    metrics
    .filter(F.col("firmware") != "2.0.1")
    .withColumn("next_temp_c_minus_pressure_adjusted", F.lead("temp_c_minus_pressure_adjusted", 1).over(w5))
    .withColumn("running_sum_status_bits_bucket_count_2h", F.count("running_sum_status_bits_bucket").over(w6))
)

result = (
    metrics_device_readings
    .withColumn("hour_window", F.date_trunc("HOUR", "reading_ts"))
    .withColumn("running_count_status_bits_bucket_tier", F.when(F.col("running_count_status_bits_bucket") >= 13, "LARGE").otherwise("SMALL"))
    .withColumn("activity_day", F.date_trunc("DAY", "reading_ts"))
    .groupingSets(
        [["hour_window", "running_count_status_bits_bucket_tier", "activity_day"], ["hour_window", "running_count_status_bits_bucket_tier"]],
        "hour_window",
        "running_count_status_bits_bucket_tier",
        "activity_day",
    )
    .agg(
        F.percentile_approx("corr_humidity_pressure", [0.25, 0.5, 0.75]).alias("corr_humidity_pressure_quartiles"),
        F.percentile_approx("pressure_whole", [0.25, 0.5, 0.75]).alias("pressure_whole_quartiles"),
        F.min("reading_ts").alias("first_seen"),
        F.percentile_approx("running_max_status_bits_bucket", [0.25, 0.5, 0.75]).alias("running_max_status_bits_bucket_quartiles"),
        F.percentile_approx("pressure", 0.9).alias("pressure_p90"),
        F.round(F.avg("running_count_status_bits_bucket"), 2).alias("avg_running_count_status_bits_bucket"),
    )
    .filter(F.col("avg_running_count_status_bits_bucket") >= 0)
    .orderBy(F.col("pressure_p90").desc_nulls_last())
)
