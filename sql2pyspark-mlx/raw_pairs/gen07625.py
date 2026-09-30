from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("customer_id")
    .agg(
        F.count(F.when(F.col("status") == "PENDING", 1)).alias("pending_count"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.stddev_samp("amount").alias("amount_stddev_2"),
        F.stddev_samp("discount").alias("discount_stddev"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.percentile_approx("amount", 0.95).alias("amount_p95"),
    )
    .orderBy("amount_p95")
)
