from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("WEEK", "reading_ts")).orderBy("pressure", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("pressure_sum_1h").asc_nulls_first())
w6 = Window.orderBy(F.col("running_humidity").asc_nulls_first())
w7 = Window.orderBy(F.col("status_bits").desc())
w8 = Window.orderBy("running_max_running_humidity")
w9 = Window.orderBy(F.col("running_humidity").desc_nulls_first())
w10 = Window.orderBy(F.col("status_bits_percent_rank").desc())
w11 = Window.orderBy("status_bits")
w12 = Window.orderBy(F.col("pressure_sum_1h_rank").desc())
w13 = Window.orderBy(F.col("status_bits_percent_rank").asc_nulls_first())

cleaned = (
    df_device_readings
    .filter(F.col("status_bits") < 4)
    .select(
        "id",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.length("device_code").alias("device_code_length"),
    )
)

scored = (
    cleaned
    .filter(F.col("humidity") > 37)
    .select(
        "id",
        "reading_ts",
        "humidity",
        "status_bits",
        "firmware",
        F.sum("pressure").over(w_grp).alias("pressure_sum_1h"),
        F.avg("status_bits").over(w2).alias("status_bits_avg_3"),
        F.sum("humidity").over(w3).alias("running_humidity"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "humidity",
        "status_bits",
        "firmware",
        "pressure_sum_1h",
        "running_humidity",
        F.max("running_humidity").over(w_ordered).alias("running_max_running_humidity"),
        F.count("running_humidity").over(w_ordered).alias("running_count_running_humidity"),
        F.min("running_humidity").over(w_ordered).alias("running_min_running_humidity"),
    )
)

remaining_keys = (
    metrics
    .select("id", "firmware")
    .subtract(
        df_device_readings
        .filter(F.col("firmware").isin(["2.0.1", "1.0.3"]))
        .select("id", "firmware")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.firmware") == F.col("k.firmware")), "inner")
    .select("s.*")
)

cleaned_device_readings = (
    kept_rows
    .filter(F.col("running_humidity").between(225, 258))
    .select(
        "id",
        "humidity",
        "status_bits",
        "firmware",
        "pressure_sum_1h",
        "running_humidity",
        "running_max_running_humidity",
        "running_count_running_humidity",
        "running_min_running_humidity",
        F.when(F.col("running_max_running_humidity") >= 380, "High").when(F.col("running_max_running_humidity") >= 140, "Medium").otherwise("Low").alias("running_max_running_humidity_tier"),
        F.greatest("pressure_sum_1h", "running_max_running_humidity").alias("greatest_pressure_sum_1h_running_max_running_humidity"),
    )
)

metrics_device_readings = (
    cleaned_device_readings
    .select(
        "*",
        F.cume_dist().over(w5).alias("pressure_sum_1h_cume_dist"),
        F.cume_dist().over(w6).alias("running_humidity_cume_dist"),
        F.percent_rank().over(w7).alias("status_bits_percent_rank"),
        F.cume_dist().over(w8).alias("running_max_running_humidity_cume_dist"),
        F.rank().over(w5).alias("pressure_sum_1h_rank"),
    )
)

metrics_device_readings_2 = (
    metrics_device_readings
    .withColumn("running_humidity_cume_dist_2", F.cume_dist().over(w9))
    .withColumn("status_bits_percent_rank_cume_dist", F.cume_dist().over(w10))
)

result = (
    metrics_device_readings_2
    .select(
        "id",
        "humidity",
        "status_bits",
        "firmware",
        "running_humidity",
        "running_max_running_humidity",
        "running_max_running_humidity_tier",
        "running_humidity_cume_dist",
        "status_bits_percent_rank",
        "running_max_running_humidity_cume_dist",
        "pressure_sum_1h_rank",
        "running_humidity_cume_dist_2",
        F.rank().over(w11).alias("status_bits_rank"),
        F.cume_dist().over(w12).alias("pressure_sum_1h_rank_cume_dist"),
        F.rank().over(w13).alias("status_bits_percent_rank_rank"),
    )
)
