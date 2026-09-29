from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "outer")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter((F.col("o.shipping_fee") >= 31) & (F.col("p.stock_qty") == 169))
    .select(
        F.col("oi.order_id"),
        F.col("oi.product_id"),
        F.col("o.store_id"),
        F.col("o.shipping_fee"),
        F.col("p.price"),
        F.col("p.launch_date"),
    )
)
