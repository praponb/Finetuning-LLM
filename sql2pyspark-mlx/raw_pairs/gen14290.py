from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("temp_c").asc_nulls_last(), "id")
w2 = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc())
w_ordered = Window.partitionBy("device_code").orderBy("latest_rank", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.orderBy("humidity", "id")
w5 = Window.orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("pressure", "id")
w7 = Window.orderBy("last3_max_status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy("last3_count_status_bits", "id").rowsBetween(-3, Window.currentRow)
w10 = Window.orderBy("last3_sum_status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("humidity") < 53)
    .withColumn("humidity_rounded", F.round("humidity", 0))
)

deduped = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

latest = (
    deduped
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

metrics = (
    latest
    .select(
        "id",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "humidity_rounded",
        F.sum("status_bits").over(w_ordered).alias("last3_sum_status_bits"),
        F.max("status_bits").over(w_ordered).alias("last3_max_status_bits"),
        F.count("status_bits").over(w_ordered).alias("last3_count_status_bits"),
    )
)

cleaned = (
    metrics
    .filter(F.col("last3_max_status_bits") < 8.5)
    .select(
        "*",
        (F.col("last3_count_status_bits") * F.col("pressure")).alias("last3_count_status_bits_times_pressure"),
    )
)

scored = (
    cleaned
    .filter(F.col("humidity_rounded").isNull())
    .withColumn("prev_last3_count_status_bits_times_pressure", F.lag("last3_count_status_bits_times_pressure", 1).over(w4))
    .withColumn("last_known_pressure", F.last("pressure", ignorenulls=True).over(w5))
    .withColumn("prev_last3_count_status_bits_times_pressure_2", F.lag("last3_count_status_bits_times_pressure", 2).over(w6))
    .withColumn("cumulative_temp_c", F.sum("temp_c").over(w7))
    .withColumn("cumulative_temp_c_2", F.sum("temp_c").over(w8))
)

scored_device_readings = (
    scored
    .select(
        "*",
        F.count("prev_last3_count_status_bits_times_pressure_2").over(w_hist).alias("last4_count_prev_last3_count_status_bits_times_pressure_2"),
        F.sum("prev_last3_count_status_bits_times_pressure_2").over(w_hist).alias("last4_sum_prev_last3_count_status_bits_times_pressure_2"),
        F.min("prev_last3_count_status_bits_times_pressure_2").over(w_hist).alias("last4_min_prev_last3_count_status_bits_times_pressure_2"),
        F.max("prev_last3_count_status_bits_times_pressure_2").over(w_hist).alias("last4_max_prev_last3_count_status_bits_times_pressure_2"),
        F.first("last3_count_status_bits", ignorenulls=True).over(w10).alias("first_known_last3_count_status_bits"),
    )
)

result = (
    scored_device_readings
    .groupBy(
        F.when(F.col("temp_c") >= 35.1, "LARGE").otherwise("SMALL").alias("temp_c_tier"),
        F.when(F.col("pressure") >= 1019, "LARGE").otherwise("SMALL").alias("pressure_tier"),
    )
    .agg(
        F.percentile_approx("status_bits", [0.25, 0.5, 0.75]).alias("status_bits_quartiles"),
        F.countDistinct("firmware").alias("distinct_firmwares"),
        F.round(F.avg("last4_max_prev_last3_count_status_bits_times_pressure_2"), 2).alias("avg_last4_max_prev_last3_count_status_bits_times_pressure_2"),
        F.percentile_approx("last4_sum_prev_last3_count_status_bits_times_pressure_2", 0.95).alias("last4_sum_prev_last3_count_status_bits_times_pressure_2_p95"),
        F.percentile_approx("last_known_pressure", [0.25, 0.5, 0.75]).alias("last_known_pressure_quartiles"),
    )
)
