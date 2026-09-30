from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel", "status").orderBy("shipping_fee_int", "id")
w3 = Window.partitionBy("customer_id", "channel")
w4 = Window.partitionBy(F.date_trunc("week", "order_date")).orderBy("shipping_fee", "id")
w5 = Window.partitionBy(F.year("order_date"), "store_id").orderBy("shipping_fee", "id").rowsBetween(-5, -1)
shipping_fee_by_store_id = df_orders.groupBy("store_id").agg(F.min("shipping_fee").alias("min_order_shipping_fee"))

filtered = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        F.col("shipping_fee").cast("int").alias("shipping_fee_int"),
        F.lower("channel").alias("channel_lower"),
    )
)

metrics = (
    filtered
    .withColumn("running_sum_amount", F.sum("amount").over(w))
    .withColumn("running_avg_amount", F.avg("amount").over(w))
    .withColumn("running_max_amount", F.max("amount").over(w))
    .withColumn("prev_amount", F.lag("amount", 2).over(w2))
)

eligible = (
    metrics
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.shipped_date") >= F.col("s.order_date")), "left_anti")
)

with_windows = (
    eligible
    .filter(F.col("channel") == "web")
    .select(
        "id",
        "store_id",
        "channel_lower",
        "running_sum_amount",
        "running_avg_amount",
        F.round((F.col("running_sum_amount") / F.sum("running_sum_amount").over(w3)), 4).alias("running_sum_amount_share"),
        F.lag("shipping_fee_int", 1).over(w4).alias("prev_shipping_fee_int"),
        F.avg("running_max_amount").over(w5).alias("running_max_amount_avg_5_prior"),
    )
)

result = (
    with_windows
    .join(shipping_fee_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "channel_lower",
        "running_sum_amount_share",
        "running_max_amount_avg_5_prior",
        F.col("min_order_shipping_fee"),
    )
    .orderBy(
        F.col("running_max_amount_avg_5_prior").desc(),
        F.col("min_order_shipping_fee").desc(),
    )
)
