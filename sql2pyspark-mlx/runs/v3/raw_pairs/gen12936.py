from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .select(
        F.col("oi.unit_price"),
        F.col("p.product_name"),
        F.col("p.launch_date"),
        F.col("o.status"),
        F.col("o.shipping_fee"),
    )
    .orderBy("launch_date", "shipping_fee")
)
