from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipped_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("customer_id").orderBy("shipment_cost", "id")
discount_by_store_id = df_orders.groupBy("store_id").agg(F.sum("discount").alias("sum_order_discount"))

cleaned = (
    df_orders
    .filter((F.col("discount") <= 0.2) & (~F.col("channel").eqNullSafe(F.lit("web"))))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "channel",
        "discount",
        "shipping_fee",
        F.concat_ws("-", "status", "channel").alias("status_channel_key"),
        F.coalesce("channel", F.lit("DIRECT")).alias("resolved_channel"),
    )
)

with_last = (
    cleaned
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.shipped_date") <= F.col("s.order_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.cost").alias("shipment_cost"))
)

with_last_orders = (
    with_last
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.shipped_date") <= F.col("s.order_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.store_id",
        "s.order_date",
        "s.discount",
        "s.shipping_fee",
        "s.resolved_channel",
        "s.shipment_cost",
        F.col("c.delivered_date"),
    )
)

latest = (
    with_last_orders
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .join(discount_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "order_date",
        "shipping_fee",
        "shipment_cost",
        "delivered_date",
        "rank_in_group",
        F.col("sum_order_discount"),
    )
)
