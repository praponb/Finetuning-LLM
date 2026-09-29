from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

base = (
    df_orders
    .filter((F.year("order_date") == 2025) & (F.col("order_date") >= F.lit("2023-10-09").cast("date")))
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
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.coalesce("discount", F.lit(0)).alias("discount_filled"),
    )
)

with_latest = (
    base
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.amount",
        "s.discount",
        "s.shipping_fee",
        F.col("c.delivered_date").alias("shipment_delivered_date"),
    )
)

eligible = (
    with_latest
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.unit_price") <= 310), "left_anti")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") <= F.col("s.shipment_delivered_date")), "left_anti")
)

result = (
    not_flagged
    .select("id", "amount", "shipping_fee", "shipment_delivered_date")
    .orderBy(F.col("id").desc_nulls_last(), "amount")
)
