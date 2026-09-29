from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("least_pressure_temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.least("pressure", "temp_c").alias("least_pressure_temp_c"),
    )
)

flagged = (
    valid
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "status_bits",
        "firmware",
        "least_pressure_temp_c",
        F.sum("is_new_burst").over(w2).alias("session_id"),
    )
)

metrics = (
    islands
    .filter(F.col("firmware") == "1.1.0")
    .withColumn("first_known_least_pressure_temp_c", F.first("least_pressure_temp_c", ignorenulls=True).over(w3))
    .withColumn("first_known_least_pressure_temp_c_2", F.first("least_pressure_temp_c", ignorenulls=True).over(w3))
)

with_windows = (
    metrics
    .select(
        "id",
        "humidity",
        "first_known_least_pressure_temp_c",
        F.sum("first_known_least_pressure_temp_c_2").over(w4).alias("running_sum_first_known_least_pressure_temp_c_2"),
        F.avg("first_known_least_pressure_temp_c_2").over(w4).alias("running_avg_first_known_least_pressure_temp_c_2"),
        F.count("first_known_least_pressure_temp_c_2").over(w4).alias("running_count_first_known_least_pressure_temp_c_2"),
        F.max("first_known_least_pressure_temp_c_2").over(w4).alias("running_max_first_known_least_pressure_temp_c_2"),
    )
)

summary = (
    with_windows
    .groupBy(
        F.when(F.col("running_max_first_known_least_pressure_temp_c_2") >= 68, "LARGE").otherwise("SMALL").alias("running_max_first_known_least_pressure_temp_c_2_tier"),
        F.when(F.col("running_avg_first_known_least_pressure_temp_c_2") >= 86, "LARGE").otherwise("SMALL").alias("running_avg_first_known_least_pressure_temp_c_2_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("running_sum_first_known_least_pressure_temp_c_2", 0.95).alias("running_sum_first_known_least_pressure_temp_c_2_p95"),
        F.percentile_approx("first_known_least_pressure_temp_c", [0.25, 0.5, 0.75]).alias("first_known_least_pressure_temp_c_quartiles"),
    )
)

result = (
    summary
    .select(
        "running_max_first_known_least_pressure_temp_c_2_tier",
        "running_avg_first_known_least_pressure_temp_c_2_tier",
        "cnt",
        "running_sum_first_known_least_pressure_temp_c_2_p95",
        "first_known_least_pressure_temp_c_quartiles",
    )
    .orderBy(
        F.col("running_avg_first_known_least_pressure_temp_c_2_tier").asc_nulls_last(),
        "running_sum_first_known_least_pressure_temp_c_2_p95",
    )
)
