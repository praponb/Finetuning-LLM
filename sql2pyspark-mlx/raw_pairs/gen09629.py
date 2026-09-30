from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("shipping_fee", "id")
w2 = Window.partitionBy("customer_id", "store_id")
w3 = Window.partitionBy(F.month("order_date")).orderBy("shipping_fee", "id")
w_hist = Window.partitionBy(F.month("order_date"), "channel").orderBy(F.col("discount").desc(), "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipped_date").desc(), F.col("c.id").desc())
w_recent = Window.partitionBy("channel").orderBy("shipping_fee", "id")
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())

with_windows = (
    df_orders
    .filter(F.col("store_id") == 14)
    .select(
        "*",
        F.lag("amount", 1).over(w_running).alias("prev_amount"),
        F.corr("discount", "amount").over(w2).alias("corr_discount_amount"),
        F.ntile(10).over(w3).alias("shipping_fee_bucket"),
        F.row_number().over(w_hist).alias("event_seq"),
    )
)

with_last = (
    with_windows
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.shipped_date") <= F.col("s.order_date")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.status").alias("shipment_status"),
        F.col("c.shipped_date").alias("shipment_shipped_date"),
    )
)

eligible = (
    with_last
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.unit_price").between(150, 370)), "left_anti")
)

with_windows_orders = (
    eligible
    .select(
        "id",
        "customer_id",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        "corr_discount_amount",
        "event_seq",
        "shipment_status",
        "shipment_shipped_date",
        F.lag("discount").over(w_recent).alias("prev_discount"),
        F.lead("discount").over(w_recent).alias("next_discount"),
        F.lead("amount").over(w_recent).alias("next_amount"),
        F.lag("amount").over(w_recent).alias("prev_amount_2"),
        F.row_number().over(w_recent).alias("event_seq_2"),
    )
)

latest_match = (
    with_windows_orders
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.amount").alias("payment_amount"),
        F.col("c.method").alias("payment_method"),
    )
)

result = (
    latest_match
    .groupBy(
        F.when(F.col("event_seq_2") >= 8, "PREMIUM").otherwise("STANDARD").alias("event_seq_2_tier"),
    )
    .agg(
        F.round(F.sum("payment_amount") / F.nullif(F.count(F.when(F.col("channel") == "mobile", 1)), F.lit(0)), 2).alias("payment_amount_per_mobile"),
        F.countDistinct("channel").alias("distinct_channels"),
        F.round(F.sum("payment_amount") / F.nullif(F.count(F.when(F.col("payment_method") == "promptpay", 1)), F.lit(0)), 2).alias("payment_amount_per_promptpay"),
    )
    .orderBy("payment_amount_per_promptpay")
)
