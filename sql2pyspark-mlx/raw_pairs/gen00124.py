from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("pressure", "id")
w2 = Window.orderBy("temp_c", "id")
w3 = Window.partitionBy("device_code").orderBy(F.col("seq_no").asc_nulls_first())
w4 = Window.orderBy("status_bits", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("status_bits", "id").rowsBetween(-5, Window.currentRow)
w7 = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc())
w8 = Window.partitionBy("device_code")
w_hist = Window.partitionBy("device_code").orderBy("cumulative_status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy(F.col("status_bits_dense_rank").desc(), "id")
w11 = Window.orderBy("status_bits_dense_rank", "id")
w_running = Window.orderBy("status_bits_dense_rank_bucket", "id")
w13 = Window.orderBy(F.col("status_bits_dense_rank_bucket").desc(), "id")

metrics = (
    df_device_readings
    .select(
        "id",
        "device_code",
        "reading_ts",
        "status_bits",
        "firmware",
        F.ntile(10).over(w).alias("pressure_bucket"),
        F.row_number().over(w2).alias("row_seq"),
        F.row_number().over(w).alias("seq_no"),
    )
)

latest = (
    metrics
    .withColumn("latest_rank", F.rank().over(w3))
    .filter(F.col("latest_rank") == 1)
    .select("id", "device_code", "status_bits", "firmware", "seq_no")
)

scored = (
    latest
    .select(
        "*",
        F.avg("status_bits").over(w4).alias("status_bits_avg_6_prior"),
        F.sum("status_bits").over(w5).alias("cumulative_status_bits"),
        F.avg("status_bits").over(w6).alias("status_bits_avg_5"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "firmware",
        "seq_no",
        "cumulative_status_bits",
        "status_bits_avg_5",
        F.dense_rank().over(w7).alias("status_bits_dense_rank"),
        F.avg("cumulative_status_bits").over(w8).alias("device_code_avg_cumulative_status_bits"),
        F.last("seq_no", ignorenulls=True).over(w_hist).alias("last_known_seq_no"),
    )
)

filtered = (
    enriched
    .filter(F.col("status_bits_dense_rank") != 3)
    .select(
        "id",
        "firmware",
        "cumulative_status_bits",
        "status_bits_dense_rank",
        "device_code_avg_cumulative_status_bits",
        F.floor("device_code_avg_cumulative_status_bits").alias("device_code_avg_cumulative_status_bits_whole"),
    )
)

metrics_device_readings = (
    filtered
    .select(
        "id",
        "firmware",
        "status_bits_dense_rank",
        F.ntile(10).over(w10).alias("status_bits_dense_rank_bucket"),
        F.lag("status_bits_dense_rank", 1).over(w11).alias("prev_status_bits_dense_rank"),
    )
)

with_windows = (
    metrics_device_readings
    .filter(F.col("prev_status_bits_dense_rank").between(3, 6))
    .select(
        "id",
        "firmware",
        "status_bits_dense_rank_bucket",
        "prev_status_bits_dense_rank",
        F.lag("status_bits_dense_rank_bucket").over(w_running).alias("prev_status_bits_dense_rank_bucket"),
        F.lead("status_bits_dense_rank_bucket").over(w_running).alias("next_status_bits_dense_rank_bucket"),
        F.lead("prev_status_bits_dense_rank").over(w_running).alias("next_prev_status_bits_dense_rank"),
        F.lag("prev_status_bits_dense_rank").over(w_running).alias("prev_prev_status_bits_dense_rank"),
        F.row_number().over(w_running).alias("event_seq"),
        F.ntile(4).over(w13).alias("status_bits_dense_rank_bucket_bucket"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("prev_status_bits_dense_rank") >= 7, "PREMIUM").otherwise("STANDARD").alias("prev_status_bits_dense_rank_tier"),
        F.when(F.col("prev_prev_status_bits_dense_rank") >= 7, "LARGE").otherwise("SMALL").alias("prev_prev_status_bits_dense_rank_tier"),
    )
    .agg(
        F.round(F.avg("next_status_bits_dense_rank_bucket"), 2).alias("avg_next_status_bits_dense_rank_bucket"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("prev_status_bits_dense_rank_bucket", 0.95).alias("prev_status_bits_dense_rank_bucket_p95"),
    )
    .filter(F.col("distinct_ids") > 1)
    .orderBy(F.col("prev_status_bits_dense_rank_bucket_p95").asc_nulls_last())
)
