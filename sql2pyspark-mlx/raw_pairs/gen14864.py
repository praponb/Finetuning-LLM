from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant", "txn_type").orderBy("amount", "id")
w2 = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.orderBy("amount", "id")
w4 = Window.partitionBy("merchant").orderBy("amount_max_7d", "id").rowsBetween(-6, -1)
w_hist = Window.partitionBy("merchant")
w_part = Window.partitionBy("merchant").orderBy("amount_bucket", "id").rowsBetween(-6, Window.currentRow)

with_windows = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        F.lag("amount", 1).over(w).alias("prev_amount"),
        F.max("amount").over(w2).alias("amount_max_7d"),
    )
)

enriched = (
    with_windows
    .filter(F.col("amount").between(1500, 3000))
    .select(
        "id",
        "merchant",
        "is_flagged",
        "prev_amount",
        F.ntile(5).over(w3).alias("amount_bucket"),
        F.avg("amount_max_7d").over(w4).alias("amount_max_7d_avg_6_prior"),
    )
)

with_windows_transactions = (
    enriched
    .select(
        "*",
        F.stddev_samp("amount_bucket").over(w_hist).alias("merchant_stddev_samp_amount_bucket"),
        F.min("amount_bucket").over(w_hist).alias("merchant_min_amount_bucket"),
    )
)

filtered = (
    with_windows_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "merchant",
        "is_flagged",
        "prev_amount",
        "amount_bucket",
        "amount_max_7d_avg_6_prior",
        "merchant_stddev_samp_amount_bucket",
        F.round((F.col("merchant_stddev_samp_amount_bucket") / F.col("amount_bucket")), 2).alias("merchant_stddev_samp_amount_bucket_per_amount_bucket"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.max("amount_bucket").over(w_part).alias("last7_max_amount_bucket"),
        F.avg("amount_bucket").over(w_part).alias("last7_avg_amount_bucket"),
        F.sum("amount_bucket").over(w_part).alias("last7_sum_amount_bucket"),
    )
)

result = (
    metrics
    .groupBy("merchant")
    .agg(
        F.round(F.avg("last7_avg_amount_bucket"), 2).alias("avg_last7_avg_amount_bucket"),
        F.sum("last7_avg_amount_bucket").alias("total_last7_avg_amount_bucket"),
        F.count(F.when(F.col("merchant") == "7-Eleven", 1)).alias("7_eleven_rows"),
        F.percentile_approx("merchant_stddev_samp_amount_bucket", 0.95).alias("merchant_stddev_samp_amount_bucket_p95"),
        F.size(F.collect_set("merchant")).alias("num_merchants"),
        F.percentile_approx("merchant_stddev_samp_amount_bucket", 0.95).alias("merchant_stddev_samp_amount_bucket_p95_2"),
    )
    .orderBy(F.col("merchant_stddev_samp_amount_bucket_p95_2").asc_nulls_first())
)
