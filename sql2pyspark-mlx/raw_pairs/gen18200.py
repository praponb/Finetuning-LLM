from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.to_date("reading_ts"), "device_code").orderBy("reading_ts", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code")
w4 = Window.orderBy("cumulative_humidity_plus_humidity_avg_5", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.orderBy("cumulative_humidity_minus_humidity_avg_5", "id")

enriched = (
    df_device_readings
    .filter(F.col("humidity").between(44, 61))
    .select(
        "id",
        "temp_c",
        "humidity",
        F.avg("humidity").over(w).alias("humidity_avg_5"),
        F.sum("humidity").over(w2).alias("cumulative_humidity"),
        F.stddev_samp("temp_c").over(w3).alias("device_code_stddev_samp_temp_c"),
    )
)

base = (
    enriched
    .filter(F.col("humidity") > 37)
    .select(
        "id",
        "humidity",
        "humidity_avg_5",
        "cumulative_humidity",
        F.round((F.col("temp_c") * 1.15), 2).alias("temp_c_adjusted"),
        (F.col("cumulative_humidity") - F.col("humidity_avg_5")).alias("cumulative_humidity_minus_humidity_avg_5"),
    )
)

valid = (
    base
    .filter(F.col("temp_c_adjusted") > 47)
    .select(
        "id",
        "cumulative_humidity",
        "temp_c_adjusted",
        "cumulative_humidity_minus_humidity_avg_5",
        (F.col("cumulative_humidity") + F.col("humidity_avg_5")).alias("cumulative_humidity_plus_humidity_avg_5"),
    )
)

scored = (
    valid
    .select(
        "id",
        "cumulative_humidity_plus_humidity_avg_5",
        F.sum("cumulative_humidity_minus_humidity_avg_5").over(w4).alias("last4_sum_cumulative_humidity_minus_humidity_avg_5"),
        F.count("cumulative_humidity_minus_humidity_avg_5").over(w4).alias("last4_count_cumulative_humidity_minus_humidity_avg_5"),
        F.max("cumulative_humidity_minus_humidity_avg_5").over(w4).alias("last4_max_cumulative_humidity_minus_humidity_avg_5"),
        F.avg("cumulative_humidity_minus_humidity_avg_5").over(w4).alias("last4_avg_cumulative_humidity_minus_humidity_avg_5"),
        F.lag("cumulative_humidity", 2).over(w5).alias("prev_cumulative_humidity"),
    )
)

valid_device_readings = (
    scored
    .filter(F.col("last4_avg_cumulative_humidity_minus_humidity_avg_5").isNotNull())
    .withColumn("cumulative_humidity_plus_humidity_avg_5_per_last4_max_cumulative_humidity_minus_humidity_avg_5", (F.col("cumulative_humidity_plus_humidity_avg_5") / F.nullif("last4_max_cumulative_humidity_minus_humidity_avg_5", F.lit(0))))
)

result = (
    valid_device_readings
    .select(
        "id",
        "last4_sum_cumulative_humidity_minus_humidity_avg_5",
        "last4_count_cumulative_humidity_minus_humidity_avg_5",
        "last4_max_cumulative_humidity_minus_humidity_avg_5",
        "last4_avg_cumulative_humidity_minus_humidity_avg_5",
        "prev_cumulative_humidity",
    )
    .orderBy(F.col("id").desc_nulls_last())
)
