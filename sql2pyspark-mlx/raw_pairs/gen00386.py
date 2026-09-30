from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())
w_hist = Window.partitionBy(F.date_trunc("MONTH", "order_date"), "store_id").orderBy("discount", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy(F.year("order_date"))
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.sum("quantity").alias("sum_inventory_quantity"))

valid = (
    df_orders
    .filter(F.col("order_date") >= F.lit("2023-03-22").cast("date"))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.least("amount", "shipping_fee").alias("least_amount_shipping_fee"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_clean"),
    )
)

kept = (
    valid
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (~F.col("c.method").isin(["bank_transfer", "card", "cash"])), "left_anti")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.order_id") == 39), "left_anti")
)

with_last = (
    not_flagged
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") <= F.col("s.order_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_id",
        "s.order_date",
        "s.status",
        "s.amount",
        "s.discount",
        "s.shipping_fee",
        "s.least_amount_shipping_fee",
        "s.channel_clean",
        F.col("c.amount").alias("payment_amount"),
    )
)

with_windows = (
    with_last
    .select(
        "*",
        F.avg("payment_amount").over(w_hist).alias("payment_amount_avg_6"),
        F.max("discount").over(w3).alias("order_date_year_max_discount"),
    )
)

result = (
    with_windows
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "amount",
        "channel_clean",
        F.col("sum_inventory_quantity"),
    )
)
