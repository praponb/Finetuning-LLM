from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .select(
        F.col("oi.id"),
        F.col("oi.quantity"),
        F.col("p.launch_date"),
        F.col("p.stock_qty"),
        F.col("o.shipping_fee"),
        F.col("o.store_id"),
    )
    .orderBy("id", "stock_qty")
)
