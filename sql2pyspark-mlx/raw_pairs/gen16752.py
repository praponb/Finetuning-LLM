from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.partitionBy(F.to_date("reading_ts"), "device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("pressure", "id")
w4 = Window.orderBy("humidity_count_1h", "id")
w5 = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("last_known_humidity_sum_30m", "id")
w_ordered = Window.partitionBy(F.month("reading_ts")).orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w_hist = Window.orderBy("last_known_humidity_sum_30m", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_device_readings
    .withColumn("humidity_sum_30m", F.sum("humidity").over(w))
    .withColumn("humidity_count_1h", F.count("humidity").over(w2))
)

metrics = (
    with_windows
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        F.lag("humidity_count_1h", 1).over(w3).alias("prev_humidity_count_1h"),
        F.ntile(5).over(w4).alias("humidity_count_1h_bucket"),
        F.last("humidity_sum_30m", ignorenulls=True).over(w5).alias("last_known_humidity_sum_30m"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.lag("prev_humidity_count_1h").over(w_grp).alias("prev_prev_humidity_count_1h"),
        F.lead("prev_humidity_count_1h").over(w_grp).alias("next_prev_humidity_count_1h"),
        F.lead("humidity_count_1h_bucket").over(w_grp).alias("next_humidity_count_1h_bucket"),
        F.row_number().over(w_grp).alias("row_seq"),
        F.count("temp_c").over(w_ordered).alias("temp_c_count_15m"),
    )
)

base = (
    enriched
    .filter(F.col("last_known_humidity_sum_30m") >= 298)
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "pressure",
        "firmware",
        "prev_humidity_count_1h",
        "humidity_count_1h_bucket",
        "last_known_humidity_sum_30m",
        "prev_prev_humidity_count_1h",
        "row_seq",
        (F.col("humidity_count_1h_bucket") * F.col("temp_c")).alias("humidity_count_1h_bucket_times_temp_c"),
        F.when(F.col("next_humidity_count_1h_bucket") >= 8, "High").when(F.col("next_humidity_count_1h_bucket") >= 3, "Medium").otherwise("Low").alias("next_humidity_count_1h_bucket_level"),
    )
)

cleaned = (
    base
    .filter((F.col("reading_ts") >= F.lit("2025-09-01 03:00:00").cast("timestamp")) & (F.col("row_seq") > 6))
    .select(
        "id",
        "temp_c",
        "firmware",
        "prev_humidity_count_1h",
        "humidity_count_1h_bucket",
        "last_known_humidity_sum_30m",
        "prev_prev_humidity_count_1h",
        "row_seq",
        "humidity_count_1h_bucket_times_temp_c",
        "next_humidity_count_1h_bucket_level",
        F.coalesce("humidity_count_1h_bucket", F.lit(0)).alias("humidity_count_1h_bucket_filled"),
        F.when(F.col("last_known_humidity_sum_30m") >= 395, "High").when(F.col("last_known_humidity_sum_30m") >= 149, "Medium").otherwise("Low").alias("last_known_humidity_sum_30m_level"),
    )
)

base_device_readings = (
    cleaned
    .filter(F.col("humidity_count_1h_bucket_times_temp_c") < 54)
    .select(
        "id",
        "temp_c",
        "firmware",
        "prev_humidity_count_1h",
        "last_known_humidity_sum_30m",
        "row_seq",
        "humidity_count_1h_bucket_times_temp_c",
        "next_humidity_count_1h_bucket_level",
        "humidity_count_1h_bucket_filled",
        "last_known_humidity_sum_30m_level",
        F.when(F.col("row_seq") >= 7, "High").when(F.col("row_seq") >= 3, "Medium").otherwise("Low").alias("row_seq_level"),
    )
)

result = (
    base_device_readings
    .withColumn("running_min_humidity_count_1h_bucket_filled", F.min("humidity_count_1h_bucket_filled").over(w_hist))
    .withColumn("running_avg_humidity_count_1h_bucket_filled", F.avg("humidity_count_1h_bucket_filled").over(w_hist))
    .withColumn("running_sum_humidity_count_1h_bucket_filled", F.sum("humidity_count_1h_bucket_filled").over(w_hist))
    .withColumn("running_humidity_count_1h_bucket_filled", F.sum("humidity_count_1h_bucket_filled").over(w_hist))
)
