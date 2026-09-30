from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "status")
w2 = Window.partitionBy("channel")
w3 = Window.partitionBy("status")
w4 = Window.partitionBy("customer_id")
w_running = Window.partitionBy("status", "customer_id")
w6 = Window.orderBy(F.col("corr_shipping_fee_discount_2").desc_nulls_last())
w7 = Window.orderBy(F.col("amount").desc())
w8 = Window.orderBy(F.col("status_stddev_samp_corr_shipping_fee_discount_2").desc_nulls_first())
w9 = Window.partitionBy("store_id").orderBy(F.col("shipping_fee").asc_nulls_last())

remaining_keys = (
    df_orders
    .select("customer_id", "channel")
    .subtract(
        df_orders
        .filter(F.col("customer_id").isin([11, 28, 33]))
        .select("customer_id", "channel")
    )
)

reconciled = (
    df_orders
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .withColumn("corr_shipping_fee_discount", F.corr("shipping_fee", "discount").over(w))
    .withColumn("corr_shipping_fee_discount_2", F.corr("shipping_fee", "discount").over(w2))
    .withColumn("status_count_discount", F.count("discount").over(w3))
    .withColumn("status_stddev_samp_amount", F.stddev_samp("amount").over(w3))
)

with_windows_orders = (
    with_windows
    .select(
        "*",
        F.corr("discount", "amount").over(w3).alias("corr_discount_amount"),
        F.sum("corr_shipping_fee_discount").over(w4).alias("customer_id_sum_corr_shipping_fee_discount"),
        F.stddev_samp("corr_shipping_fee_discount_2").over(w3).alias("status_stddev_samp_corr_shipping_fee_discount_2"),
        F.corr("shipping_fee", "amount").over(w_running).alias("corr_shipping_fee_amount"),
    )
)

enriched = (
    with_windows_orders
    .withColumn("status_max_corr_shipping_fee_discount", F.max("corr_shipping_fee_discount").over(w3))
    .withColumn("corr_shipping_fee_discount_2_dense_rank", F.dense_rank().over(w6))
    .withColumn("amount_rank", F.rank().over(w7))
)

enriched_orders = (
    enriched
    .withColumn("store_id_count_corr_shipping_fee_discount_2_dense_rank", F.count("corr_shipping_fee_discount_2_dense_rank").over(w))
    .withColumn("status_stddev_samp_corr_shipping_fee_discount_2_percent_rank", F.percent_rank().over(w8))
    .withColumn("shipping_fee_cume_dist", F.cume_dist().over(w9))
)

daily = (
    enriched_orders
    .withColumn("corr_shipping_fee_discount_2_tier", F.when(F.col("corr_shipping_fee_discount_2") >= 0.22, "HIGH").otherwise("LOW"))
    .withColumn("store_id_count_corr_shipping_fee_discount_2_dense_rank_tier", F.when(F.col("store_id_count_corr_shipping_fee_discount_2_dense_rank") >= 16, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["corr_shipping_fee_discount_2_tier", "store_id_count_corr_shipping_fee_discount_2_dense_rank_tier", "customer_id"], ["corr_shipping_fee_discount_2_tier", "store_id_count_corr_shipping_fee_discount_2_dense_rank_tier"], []],
        "corr_shipping_fee_discount_2_tier",
        "store_id_count_corr_shipping_fee_discount_2_dense_rank_tier",
        "customer_id",
    )
    .agg(
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_rows"),
        F.avg("status_count_discount").alias("avg_status_count_discount"),
        F.round(F.sum("corr_discount_amount") / F.nullif(F.count(F.when(F.col("channel") == "mobile", 1)), F.lit(0)), 2).alias("corr_discount_amount_per_mobile"),
    )
    .filter(F.col("partner_rows") > 1)
)

result = (
    daily
    .select(
        "corr_shipping_fee_discount_2_tier",
        "store_id_count_corr_shipping_fee_discount_2_dense_rank_tier",
        "customer_id",
        "avg_status_count_discount",
        "corr_discount_amount_per_mobile",
    )
    .orderBy(F.col("corr_shipping_fee_discount_2_tier").desc_nulls_first())
)
