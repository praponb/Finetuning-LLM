from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("status").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy("method").orderBy("running_sum_shipping_fee", "id")
w4 = Window.partitionBy("status").orderBy("running_max_shipping_fee")

with_dims = (
    df_payments
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "inner")
    .filter(F.col("p.shipping_fee").between(23, 37))
    .select("src.*", F.col("p.shipping_fee"), F.col("p.amount").alias("order_amount"))
)

enriched = (
    with_dims
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "shipping_fee",
        F.max("shipping_fee").over(w_ordered).alias("running_max_shipping_fee"),
        F.sum("shipping_fee").over(w_ordered).alias("running_sum_shipping_fee"),
        F.avg("shipping_fee").over(w_ordered).alias("running_avg_shipping_fee"),
        F.min("shipping_fee").over(w_ordered).alias("running_min_shipping_fee"),
        F.max("order_amount").over(w2).alias("order_amount_max_30d"),
    )
)

with_dims_payments = (
    enriched
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.status"), F.col("b.order_date"))
)

metrics = (
    with_dims_payments
    .select(
        "id",
        "order_id",
        "method",
        "shipping_fee",
        "running_max_shipping_fee",
        "running_min_shipping_fee",
        "status",
        F.lag("running_avg_shipping_fee").over(w3).alias("prev_running_avg_shipping_fee"),
        F.lead("running_avg_shipping_fee").over(w3).alias("next_running_avg_shipping_fee"),
        F.lead("running_min_shipping_fee").over(w3).alias("next_running_min_shipping_fee"),
        F.lag("running_min_shipping_fee").over(w3).alias("prev_running_min_shipping_fee"),
        F.row_number().over(w3).alias("row_seq"),
        F.dense_rank().over(w4).alias("running_max_shipping_fee_dense_rank"),
    )
)

result = (
    metrics
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "inner")
    .groupBy(
        F.col("o.channel").alias("channel"),
        F.col("src.status").alias("payment_status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("src.row_seq")).alias("max_row_seq"),
    )
)
