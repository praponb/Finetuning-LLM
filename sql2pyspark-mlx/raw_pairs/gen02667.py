from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("customer_id", "status").orderBy("discount", "id")
w2 = Window.partitionBy("customer_id").orderBy("days_since_order_date", "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("status").orderBy(F.col("prev_discount").desc())

filtered = (
    df_orders
    .filter(F.col("store_id").isin([12, 19]))
    .select(
        "id",
        "customer_id",
        "status",
        "channel",
        "discount",
        "shipping_fee",
        F.datediff(F.current_date(), "order_date").alias("days_since_order_date"),
        F.add_months("order_date", 12).alias("order_date_plus_months"),
    )
)

with_windows = (
    filtered
    .withColumn("prev_discount", F.lag("discount").over(w_hist))
    .withColumn("next_shipping_fee", F.lead("shipping_fee").over(w_hist))
    .withColumn("row_seq", F.row_number().over(w_hist))
    .withColumn("days_since_order_date_bucket", F.ntile(3).over(w2))
)

with_latest = (
    with_windows
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.status",
        "s.discount",
        "s.shipping_fee",
        "s.prev_discount",
        "s.next_shipping_fee",
        "s.days_since_order_date_bucket",
        F.col("c.unit_price"),
        F.col("c.quantity"),
    )
)

deduped = (
    with_latest
    .withColumn("rank_in_group", F.rank().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "prev_discount", "next_shipping_fee", "unit_price", "quantity")
)

eligible = (
    deduped
    .alias("s")
    .join(df_shipments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.cost") <= 42), "left_anti")
)

report = (
    eligible
    .select("id", "prev_discount", "quantity")
    .orderBy(F.col("prev_discount").desc_nulls_last())
)

result = report.select("id", "prev_discount", "quantity")
