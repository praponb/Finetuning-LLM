from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id", "customer_id").orderBy("order_item_unit_price", "id")
w4 = Window.partitionBy("channel")
w5 = Window.partitionBy("channel").orderBy(F.col("order_item_unit_price").desc_nulls_last())
w6 = Window.partitionBy("customer_id").orderBy(F.col("order_item_unit_price").desc(), "id")
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())
w8 = Window.partitionBy("store_id").orderBy("order_item_unit_price_cume_dist")

with_last = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity"),
        F.col("c.unit_price").alias("order_item_unit_price"),
    )
)

with_windows = (
    with_last
    .withColumn("first_known_amount", F.first("amount", ignorenulls=True).over(w2))
    .withColumn("prev_discount", F.lag("discount", 1).over(w3))
    .withColumn("channel_max_discount", F.max("discount").over(w4))
    .withColumn("order_item_unit_price_cume_dist", F.cume_dist().over(w5))
    .withColumn("seq_no", F.row_number().over(w6))
)

latest_match = (
    with_windows
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount").alias("payment_amount"))
)

ranked = (
    latest_match
    .withColumn("row_num", F.rank().over(w8))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    ranked
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "amount",
        "shipping_fee",
        "order_item_unit_price",
        "first_known_amount",
        "prev_discount",
    )
    .orderBy(F.col("prev_discount").desc(), "first_known_amount")
)
