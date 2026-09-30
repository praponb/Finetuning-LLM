from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "right")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .filter(F.col("oi.quantity").between(6, 8) & F.col("oi.quantity").between(7, 8))
    .select(
        F.col("oi.id"),
        F.col("oi.quantity"),
        F.col("o.channel"),
        F.col("o.amount"),
        F.col("p.stock_qty"),
        F.round((F.col("oi.unit_price") * 0.85), 2).alias("unit_price_scaled"),
    )
)
