from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "left")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("p.launch_date") <= "2024-03-01")
    .select(
        F.col("oi.product_id"),
        F.col("oi.order_id"),
        F.col("p.category"),
        F.col("p.stock_qty"),
        F.col("o.customer_id"),
        F.col("o.shipping_fee"),
    )
)
