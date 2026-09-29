from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("channel").orderBy(F.col("amount").desc_nulls_last(), "id")
w_ordered = Window.partitionBy("channel").orderBy(F.col("amount").desc())
w4 = Window.partitionBy("customer_id", "store_id")
w5 = Window.orderBy(F.col("amount").desc(), "id")
w6 = Window.partitionBy("channel", "status")
w7 = Window.orderBy(F.col("discount").desc())

with_latest = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"))
)

ranked = (
    with_latest
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    ranked
    .withColumn("amount_cume_dist", F.cume_dist().over(w_ordered))
    .withColumn("customer_id_stddev_samp_amount", F.stddev_samp("amount").over(w4))
    .withColumn("amount_bucket", F.ntile(4).over(w5))
)

kept_keys = (
    enriched
    .select("status", "id")
    .intersectAll(
        df_orders
        .filter(F.col("discount") <= 0.36)
        .select("status", "id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .withColumn("customer_id_stddev_samp_amount_share", F.round((F.col("customer_id_stddev_samp_amount") / F.sum("customer_id_stddev_samp_amount").over(w6)), 4))
    .withColumn("discount_dense_rank", F.dense_rank().over(w7))
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.order_id").isNull()), "left_semi")
)

totals = (
    not_flagged
    .withColumn("discount_dense_rank_tier", F.when(F.col("discount_dense_rank") >= 8, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["store_id", "status", "discount_dense_rank_tier"], ["store_id", "status"], ["store_id"]],
        "store_id",
        "status",
        "discount_dense_rank_tier",
    )
    .agg(
        F.round(F.sum("discount_dense_rank") / F.nullif(F.count(F.when(F.col("status") == "RETURNED", 1)), F.lit(0)), 2).alias("discount_dense_rank_per_returned"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(F.sum("discount_dense_rank") / F.nullif(F.count(F.when(F.col("channel") == "mobile", 1)), F.lit(0)), 2).alias("discount_dense_rank_per_mobile"),
        F.sum(F.when(F.col("channel") == "web", F.col("discount"))).alias("web_total_discount"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
    )
)

result = (
    totals
    .select(
        "status",
        "discount_dense_rank_tier",
        "channel_list",
        F.when(F.col("status").rlike("^(COMPLETED|SHIPPED)$"), "GROUP_B").otherwise("GROUP_A").alias("status_zone"),
    )
)
