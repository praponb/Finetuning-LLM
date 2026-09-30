from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("running_max_humidity").desc())
w4 = Window.orderBy("running_max_humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code")
w6 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(-2, Window.currentRow)

base = (
    df_device_readings
    .filter(F.to_date("reading_ts") < "2025-02-08")
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.concat("device_code", F.lit(" "), "firmware").alias("device_code_firmware"),
    )
)

cleaned = (
    base
    .filter(F.col("temp_c").between(24, 36))
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "firmware",
        F.round("status_bits", 2).alias("status_bits_rounded"),
    )
)

scored = (
    cleaned
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "status_bits_rounded",
        F.sum("humidity").over(w).alias("running_sum_humidity"),
        F.max("humidity").over(w).alias("running_max_humidity"),
        F.min("humidity").over(w).alias("running_min_humidity"),
        F.last("temp_c", ignorenulls=True).over(w2).alias("last_known_temp_c"),
    )
)

enriched = (
    scored
    .withColumn("running_max_humidity_dense_rank", F.dense_rank().over(w3))
    .withColumn("running_temp_c", F.sum("temp_c").over(w4))
    .withColumn("device_code_max_humidity", F.max("humidity").over(w5))
)

cleaned_device_readings = (
    enriched
    .filter(F.col("temp_c") <= 22)
    .select(
        "*",
        F.col("running_temp_c").cast("string").alias("running_temp_c_str"),
        (F.col("humidity") - F.col("running_temp_c")).alias("humidity_minus_running_temp_c"),
    )
)

with_windows = (
    cleaned_device_readings
    .select(
        "id",
        "temp_c",
        "status_bits_rounded",
        "running_sum_humidity",
        "running_max_humidity_dense_rank",
        F.min("running_min_humidity").over(w6).alias("last3_min_running_min_humidity"),
        F.avg("running_min_humidity").over(w6).alias("last3_avg_running_min_humidity"),
        F.sum("running_min_humidity").over(w6).alias("last3_sum_running_min_humidity"),
    )
)

totals = (
    with_windows
    .withColumn("running_max_humidity_dense_rank_tier", F.when(F.col("running_max_humidity_dense_rank") >= 8, "LARGE").otherwise("SMALL"))
    .withColumn("last3_sum_running_min_humidity_tier", F.when(F.col("last3_sum_running_min_humidity") >= 300, "PREMIUM").otherwise("STANDARD"))
    .withColumn("temp_c_tier", F.when(F.col("temp_c") >= 40.9, "PREMIUM").otherwise("STANDARD"))
    .cube(
        "running_max_humidity_dense_rank_tier",
        "last3_sum_running_min_humidity_tier",
        "temp_c_tier",
    )
    .agg(
        F.percentile_approx("last3_sum_running_min_humidity", 0.95).alias("last3_sum_running_min_humidity_p95"),
        F.count("*").alias("num_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.percentile_approx("last3_min_running_min_humidity", 0.9).alias("last3_min_running_min_humidity_p90"),
        F.grouping("running_max_humidity_dense_rank_tier").alias("gid"),
    )
    .filter(F.col("distinct_ids_2") >= 1)
)

result = (
    totals
    .select(
        "running_max_humidity_dense_rank_tier",
        "num_rows",
        "distinct_ids",
        "distinct_ids_2",
        "last3_min_running_min_humidity_p90",
        "gid",
    )
    .orderBy(F.col("gid").desc())
)
