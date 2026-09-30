from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy(F.date_trunc("week", "reading_ts"), "device_code").orderBy("humidity", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy(F.col("humidity").asc_nulls_first())
w_part = Window.partitionBy("device_code").orderBy("humidity_avg_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.month("reading_ts"), "device_code").orderBy("humidity_avg_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("device_code").orderBy(F.col("cumulative_humidity_2").desc(), "id")
w_recent = Window.partitionBy("device_code").orderBy("running_avg_humidity_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(-3, Window.currentRow)

base = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "firmware",
        F.when(F.col("device_code").isin(["DV-01", "DV-04"]), "Tier 1").otherwise("Other").alias("device_code_group"),
    )
)

scored = (
    base
    .filter(F.col("firmware").isin(["1.0.3", "1.1.0", "2.0.1"]))
    .select(
        "*",
        F.last("humidity", ignorenulls=True).over(w_ordered).alias("last_known_humidity"),
        F.sum("humidity").over(w2).alias("cumulative_humidity"),
        F.avg("humidity").over(w_running).alias("humidity_avg_2"),
        F.dense_rank().over(w4).alias("humidity_dense_rank"),
    )
)

with_windows = (
    scored
    .withColumn("running_min_humidity_dense_rank", F.min("humidity_dense_rank").over(w_part))
    .withColumn("running_count_humidity_dense_rank", F.count("humidity_dense_rank").over(w_part))
    .withColumn("running_avg_humidity_dense_rank", F.avg("humidity_dense_rank").over(w_part))
    .withColumn("running_sum_humidity_dense_rank", F.sum("humidity_dense_rank").over(w_part))
    .withColumn("cumulative_humidity_2", F.sum("humidity").over(w6))
)

latest = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w7))
    .filter(F.col("dedup_rank") == 1)
)

scored_device_readings = (
    latest
    .select(
        "id",
        "humidity",
        "firmware",
        "last_known_humidity",
        "humidity_dense_rank",
        "running_min_humidity_dense_rank",
        "running_count_humidity_dense_rank",
        "running_avg_humidity_dense_rank",
        "running_sum_humidity_dense_rank",
        "dedup_rank",
        F.sum("humidity").over(w_recent).alias("cumulative_humidity_3"),
        F.first("cumulative_humidity_2", ignorenulls=True).over(w_recent).alias("first_known_cumulative_humidity_2"),
        F.avg("humidity").over(w9).alias("humidity_avg_3"),
    )
)

base_device_readings = (
    scored_device_readings
    .filter(F.col("running_avg_humidity_dense_rank") <= 7.3)
    .select(
        "id",
        "humidity",
        "firmware",
        "last_known_humidity",
        "running_count_humidity_dense_rank",
        "running_avg_humidity_dense_rank",
        "running_sum_humidity_dense_rank",
        "dedup_rank",
        "cumulative_humidity_3",
        "first_known_cumulative_humidity_2",
        "humidity_avg_3",
        F.col("dedup_rank").cast("double").alias("dedup_rank_str"),
    )
)

filtered = (
    base_device_readings
    .filter(F.col("dedup_rank") <= 1)
    .select(
        "id",
        "humidity",
        "last_known_humidity",
        "running_count_humidity_dense_rank",
        "running_avg_humidity_dense_rank",
        "running_sum_humidity_dense_rank",
        "dedup_rank",
        "cumulative_humidity_3",
        "first_known_cumulative_humidity_2",
        "humidity_avg_3",
        "dedup_rank_str",
        F.coalesce("humidity", F.lit(0)).alias("humidity_filled"),
    )
)

result = (
    filtered
    .withColumn("humidity_tier", F.when(F.col("humidity") >= 72, "PREMIUM").otherwise("STANDARD"))
    .withColumn("first_known_cumulative_humidity_2_tier", F.when(F.col("first_known_cumulative_humidity_2") >= 336, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["humidity_tier", "first_known_cumulative_humidity_2_tier"], ["humidity_tier"]],
        "humidity_tier",
        "first_known_cumulative_humidity_2_tier",
    )
    .agg(
        F.round(F.avg("humidity_filled"), 2).alias("avg_humidity_filled"),
        F.percentile_approx("last_known_humidity", [0.25, 0.5, 0.75]).alias("last_known_humidity_quartiles"),
    )
    .orderBy(F.col("humidity_tier").desc())
)
