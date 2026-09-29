from pyspark.sql import functions as F

kept = (
    df_orders
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.cost").between(32, 56)), "left_semi")
)

result = (
    kept
    .select("id", "store_id", "order_date", "channel", "discount", "shipping_fee")
    .orderBy("shipping_fee")
)
