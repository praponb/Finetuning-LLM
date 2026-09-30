from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w_recent = Window.partitionBy("device_code").orderBy("temp_c", "id")
w5 = Window.partitionBy("device_code").orderBy(F.col("humidity").desc_nulls_last())
w6 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w7 = Window.orderBy(F.col("burst_id").desc(), "id")
w8 = Window.partitionBy("device_code").orderBy(F.col("prev_reading_ts").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w9 = Window.partitionBy("device_code").orderBy(F.col("is_new_burst_max_15m").asc_nulls_last())
w10 = Window.partitionBy("device_code").orderBy("seq_no", "id")
w11 = Window.partitionBy("device_code").orderBy("burst_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("device_code").orderBy(F.col("humidity_cume_dist").desc())
w_ordered = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_device_readings.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

gaps = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

enriched = (
    islands
    .select(
        "id",
        "device_code",
        "reading_ts",
        "pressure",
        "prev_reading_ts",
        "burst_id",
        F.max("is_new_burst").over(w_hist).alias("is_new_burst_max_15m"),
        F.row_number().over(w_recent).alias("seq_no"),
        F.cume_dist().over(w5).alias("humidity_cume_dist"),
        F.count("burst_id").over(w6).alias("burst_id_count_30m"),
        F.ntile(5).over(w7).alias("burst_id_bucket"),
    )
)

enriched_device_readings = (
    enriched
    .withColumn("is_new_burst_max_15m_sum_7d", F.sum("is_new_burst_max_15m").over(w8))
    .withColumn("is_new_burst_max_15m_cume_dist", F.cume_dist().over(w9))
    .withColumn("seq_no_bucket", F.ntile(5).over(w10))
    .withColumn("cumulative_burst_id", F.sum("burst_id").over(w11))
    .withColumn("humidity_cume_dist_dense_rank", F.dense_rank().over(w12))
)

metrics = (
    enriched_device_readings
    .select(
        "id",
        "reading_ts",
        "pressure",
        "humidity_cume_dist",
        "burst_id_count_30m",
        "burst_id_bucket",
        "is_new_burst_max_15m_sum_7d",
        "seq_no_bucket",
        "cumulative_burst_id",
        "humidity_cume_dist_dense_rank",
        F.sum("humidity_cume_dist").over(w_ordered).alias("running_sum_humidity_cume_dist"),
        F.max("humidity_cume_dist").over(w_ordered).alias("running_max_humidity_cume_dist"),
        F.avg("humidity_cume_dist").over(w_ordered).alias("running_avg_humidity_cume_dist"),
        F.count("humidity_cume_dist").over(w_ordered).alias("running_count_humidity_cume_dist"),
    )
)

stats = (
    metrics
    .withColumn("humidity_cume_dist_tier", F.when(F.col("humidity_cume_dist") >= 0.88, "PREMIUM").otherwise("STANDARD"))
    .withColumn("running_avg_humidity_cume_dist_tier", F.when(F.col("running_avg_humidity_cume_dist") >= 0.68, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["humidity_cume_dist_tier", "cumulative_burst_id", "running_avg_humidity_cume_dist_tier"], ["humidity_cume_dist_tier", "cumulative_burst_id"], []],
        "humidity_cume_dist_tier",
        "cumulative_burst_id",
        "running_avg_humidity_cume_dist_tier",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("burst_id_bucket", 0.95).alias("burst_id_bucket_p95"),
        F.percentile_approx("pressure", 0.95).alias("pressure_p95"),
    )
    .filter(F.col("burst_id_bucket_p95") >= 0)
)

result = (
    stats
    .select(
        "humidity_cume_dist_tier",
        "running_avg_humidity_cume_dist_tier",
        "burst_id_bucket_p95",
        "pressure_p95",
    )
    .orderBy(
        F.col("pressure_p95").desc_nulls_first(),
        F.col("burst_id_bucket_p95").desc(),
    )
)
