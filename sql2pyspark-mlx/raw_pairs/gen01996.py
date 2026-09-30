from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("last3_count_pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("running_min_status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.to_date("reading_ts")).orderBy(F.col("running_max_status_bits").asc_nulls_last())
w5 = Window.orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("running_min_status_bits").desc_nulls_last())
w7 = Window.orderBy("cumulative_running_min_status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("running_min_status_bits", "id")

cleaned = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01 07:00:00").cast("timestamp")) & F.col("humidity").between(36, 69))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        (F.col("humidity") * F.col("status_bits")).alias("humidity_times_status_bits"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.sum("pressure").over(w).alias("last3_sum_pressure"),
        F.count("pressure").over(w).alias("last3_count_pressure"),
        F.avg("pressure").over(w).alias("last3_avg_pressure"),
    )
)

enriched_device_readings = (
    enriched
    .filter(F.col("humidity") < 37)
    .select(
        "id",
        "reading_ts",
        "humidity",
        "last3_count_pressure",
        "last3_avg_pressure",
        F.max("status_bits").over(w2).alias("running_max_status_bits"),
        F.sum("status_bits").over(w2).alias("running_sum_status_bits"),
        F.min("status_bits").over(w2).alias("running_min_status_bits"),
    )
)

enriched_device_readings_2 = (
    enriched_device_readings
    .select(
        "id",
        "humidity",
        "last3_count_pressure",
        "last3_avg_pressure",
        "running_max_status_bits",
        "running_sum_status_bits",
        "running_min_status_bits",
        F.first("last3_avg_pressure", ignorenulls=True).over(w3).alias("first_known_last3_avg_pressure"),
        F.dense_rank().over(w4).alias("running_max_status_bits_dense_rank"),
    )
)

enriched_device_readings_3 = (
    enriched_device_readings_2
    .select(
        "*",
        F.sum("running_min_status_bits").over(w5).alias("cumulative_running_min_status_bits"),
        F.dense_rank().over(w6).alias("running_min_status_bits_dense_rank"),
    )
)

enriched_device_readings_4 = (
    enriched_device_readings_3
    .withColumn("running_avg_last3_avg_pressure", F.avg("last3_avg_pressure").over(w7))
    .withColumn("running_count_last3_avg_pressure", F.count("last3_avg_pressure").over(w7))
    .withColumn("running_max_last3_avg_pressure", F.max("last3_avg_pressure").over(w7))
    .withColumn("prev_cumulative_running_min_status_bits", F.lag("cumulative_running_min_status_bits", 1).over(w8))
)

totals = (
    enriched_device_readings_4
    .withColumn("last3_count_pressure_tier", F.when(F.col("last3_count_pressure") >= 15, "LARGE").otherwise("SMALL"))
    .withColumn("humidity_tier", F.when(F.col("humidity") >= 74, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["last3_count_pressure_tier", "humidity_tier"], ["last3_count_pressure_tier"], []],
        "last3_count_pressure_tier",
        "humidity_tier",
    )
    .agg(
        F.percentile_approx("first_known_last3_avg_pressure", [0.25, 0.5, 0.75]).alias("first_known_last3_avg_pressure_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .filter(F.col("distinct_ids_2") > 1)
)

result = (
    totals
    .select(
        "last3_count_pressure_tier",
        "humidity_tier",
        "first_known_last3_avg_pressure_quartiles",
        "distinct_ids",
    )
    .orderBy(F.col("last3_count_pressure_tier").desc(), "humidity_tier")
)
