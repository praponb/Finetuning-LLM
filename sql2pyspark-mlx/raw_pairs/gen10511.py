from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("method", "channel").orderBy("amount", "id")
w3 = Window.partitionBy("method").orderBy("shipping_fee", "id")
w_hist = Window.partitionBy("order_status", "method").orderBy("amount_bucket", "id")
w5 = Window.partitionBy("order_status").orderBy(F.col("amount_bucket").desc_nulls_last(), "id")
w6 = Window.orderBy("amount_bucket", "id")
w7 = Window.orderBy("next_amount_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("order_status").orderBy("next_shipping_fee_bucket", "id").rowsBetween(-2, -1)
w9 = Window.partitionBy("order_status").orderBy("amount_bucket", "id").rowsBetween(-2, -1)
w10 = Window.partitionBy("order_status").orderBy(F.col("amount_bucket_bucket").desc())
w11 = Window.partitionBy("order_status").orderBy("next_amount_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("order_status")
w13 = Window.orderBy("running_seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w14 = Window.orderBy(F.col("order_status_sum_next_amount_bucket").desc(), "id")
w15 = Window.orderBy("seq_no_avg_2_prior", "id")

with_dims = (
    df_payments
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "inner")
    .select(
        "e.id",
        "e.order_id",
        "e.method",
        "e.amount",
        "e.status",
        F.col("o.channel"),
        F.col("o.shipping_fee"),
        F.col("o.status").alias("order_status"),
    )
)

enriched = (
    with_dims
    .select(
        "id",
        "method",
        "order_status",
        F.last("order_status", ignorenulls=True).over(w).alias("last_known_order_status"),
        F.ntile(5).over(w2).alias("amount_bucket"),
        F.ntile(5).over(w3).alias("shipping_fee_bucket"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "order_status",
        "last_known_order_status",
        "amount_bucket",
        F.lead("shipping_fee_bucket").over(w_hist).alias("next_shipping_fee_bucket"),
        F.lead("amount_bucket").over(w_hist).alias("next_amount_bucket"),
        F.row_number().over(w_hist).alias("seq_no"),
    )
)

deduped = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_windows = (
    deduped
    .select(
        "*",
        F.ntile(3).over(w6).alias("amount_bucket_bucket"),
        F.sum("seq_no").over(w7).alias("running_seq_no"),
        F.avg("next_shipping_fee_bucket").over(w8).alias("next_shipping_fee_bucket_avg_2_prior"),
        F.avg("seq_no").over(w9).alias("seq_no_avg_2_prior"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "order_status",
        "last_known_order_status",
        "amount_bucket",
        "seq_no",
        "amount_bucket_bucket",
        "seq_no_avg_2_prior",
        F.dense_rank().over(w10).alias("amount_bucket_bucket_dense_rank"),
        F.last("running_seq_no", ignorenulls=True).over(w11).alias("last_known_running_seq_no"),
        F.sum("next_amount_bucket").over(w12).alias("order_status_sum_next_amount_bucket"),
        F.last("next_amount_bucket", ignorenulls=True).over(w13).alias("last_known_next_amount_bucket"),
    )
)

enriched_payments = (
    metrics
    .select(
        "*",
        F.ntile(4).over(w14).alias("order_status_sum_next_amount_bucket_bucket"),
        F.lead("seq_no_avg_2_prior", 2, 0).over(w15).alias("next_seq_no_avg_2_prior"),
        F.round((F.col("last_known_next_amount_bucket") / F.sum("last_known_next_amount_bucket").over(w12)), 4).alias("last_known_next_amount_bucket_share"),
    )
)

result = (
    enriched_payments
    .groupBy(
        F.when(F.col("seq_no_avg_2_prior") >= 7, "HIGH").otherwise("LOW").alias("seq_no_avg_2_prior_tier"),
        "last_known_running_seq_no",
        F.when(F.col("amount_bucket_bucket") >= 8, "LARGE").otherwise("SMALL").alias("amount_bucket_bucket_tier"),
    )
    .agg(
        F.count(F.when(F.col("order_status") == "CANCELLED", 1)).alias("cancelled_count"),
        F.percentile_approx("amount_bucket_bucket_dense_rank", 0.5).alias("amount_bucket_bucket_dense_rank_p50"),
        F.sort_array(F.collect_set("order_status")).alias("order_status_list"),
    )
    .orderBy(F.col("amount_bucket_bucket_dense_rank_p50").desc_nulls_first())
)
