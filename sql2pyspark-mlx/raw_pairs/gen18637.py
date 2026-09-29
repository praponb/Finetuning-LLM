from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel", "status").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("channel").orderBy(F.col("shipping_fee").desc(), "id")
w5 = Window.partitionBy("customer_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("channel").orderBy(F.col("cumulative_shipping_fee_2").desc(), "id")
w7 = Window.partitionBy("customer_id").orderBy("amount", "id").rowsBetween(-3, -1)
amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("amount").alias("sum_order_amount"))

filtered = (
    df_orders
    .filter(F.col("order_date") >= F.lit("2023-08-10").cast("date"))
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.concat_ws("/", "channel", "status").alias("channel_status_key"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_clean"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.sum("shipping_fee").over(w).alias("cumulative_shipping_fee"),
        F.sum("discount").over(w2).alias("running_discount"),
        F.sum("shipping_fee").over(w3).alias("cumulative_shipping_fee_2"),
        F.ntile(10).over(w4).alias("shipping_fee_bucket"),
    )
)

enriched = (
    metrics
    .select(
        "id",
        "customer_id",
        "store_id",
        "channel",
        "amount",
        "discount",
        "channel_clean",
        "cumulative_shipping_fee",
        "running_discount",
        F.sum("discount").over(w5).alias("running_discount_2"),
        F.row_number().over(w6).alias("seq_no"),
        F.avg("amount").over(w7).alias("amount_avg_3_prior"),
    )
)

enriched_orders = (
    enriched
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .filter(F.col("b.is_active"))
    .select("a.*", F.col("b.city"), F.col("b.segment"))
)

with_dims = (
    enriched_orders
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.region"))
)

result = (
    with_dims
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "store_id",
        "channel",
        "amount",
        "discount",
        "channel_clean",
        "cumulative_shipping_fee",
        "running_discount_2",
        F.col("sum_order_amount"),
    )
    .orderBy("sum_order_amount")
)
