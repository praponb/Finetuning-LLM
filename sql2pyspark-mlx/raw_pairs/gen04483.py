from pyspark.sql import functions as F

unit_price_by_order_id = df_order_items.groupBy("order_id").agg(F.sum("unit_price").alias("sum_order_item_unit_price"))

joined = (
    df_shipments
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.shipping_fee"), F.col("d.order_date"))
)

result = (
    joined
    .join(unit_price_by_order_id, on="order_id", how="left")
    .select("id", "order_id", "cost", "order_date", F.col("sum_order_item_unit_price"))
    .orderBy(F.col("cost").desc_nulls_first())
)
