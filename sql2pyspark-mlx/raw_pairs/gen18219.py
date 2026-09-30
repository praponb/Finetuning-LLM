from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "order_id").orderBy("order_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("discount", "id")
w3 = Window.orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("order_id", "channel").orderBy("discount_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id")
w6 = Window.orderBy("running_max_discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("running_avg_discount", "id")

with_dims = (
    df_payments
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "left")
    .filter(F.col("b.channel") == "web")
    .select("a.*", F.col("b.channel"), F.col("b.shipping_fee"))
)

with_dims_payments = (
    with_dims
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.method",
        "src.amount",
        "src.status",
        "src.shipping_fee",
        F.col("o.amount").alias("order_amount"),
    )
)

enriched = (
    with_dims_payments
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.amount",
        "src.shipping_fee",
        "src.order_amount",
        F.col("o.channel"),
        F.col("o.discount"),
    )
)

enriched_payments = (
    enriched
    .select(
        "*",
        F.last("shipping_fee", ignorenulls=True).over(w).alias("last_known_shipping_fee"),
        F.ntile(5).over(w2).alias("discount_bucket"),
        F.sum("amount").over(w3).alias("cumulative_amount"),
    )
)

enriched_payments_2 = (
    enriched_payments
    .select(
        "id",
        "shipping_fee",
        "discount_bucket",
        "cumulative_amount",
        F.sum("discount").over(w_hist).alias("running_sum_discount"),
        F.max("discount").over(w_hist).alias("running_max_discount"),
        F.avg("discount").over(w_hist).alias("running_avg_discount"),
        F.count("discount_bucket").over(w5).alias("order_id_count_discount_bucket"),
    )
)

with_windows = (
    enriched_payments_2
    .select(
        "id",
        "shipping_fee",
        "discount_bucket",
        "cumulative_amount",
        "running_sum_discount",
        "running_avg_discount",
        "order_id_count_discount_bucket",
        F.last("order_id_count_discount_bucket", ignorenulls=True).over(w6).alias("last_known_order_id_count_discount_bucket"),
        F.lag("cumulative_amount", 1).over(w7).alias("prev_cumulative_amount"),
    )
)

result = (
    with_windows
    .select(
        "id",
        F.col("running_sum_discount").alias("RUNNING_SUM_DISCOUNT_VAL"),
        F.col("shipping_fee").alias("SHIPPING_FEE_VAL"),
        F.col("running_avg_discount").alias("RUNNING_AVG_DISCOUNT"),
    )
    .unpivot(
        ids=["id"],
        values=["RUNNING_SUM_DISCOUNT_VAL", "SHIPPING_FEE_VAL", "RUNNING_AVG_DISCOUNT"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
