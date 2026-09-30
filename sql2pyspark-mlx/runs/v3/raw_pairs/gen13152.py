from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "left")
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("o.shipping_fee"),
        F.col("p.stock_qty"),
    )
    .orderBy("shipping_fee", F.col("stock_qty").desc())
)
