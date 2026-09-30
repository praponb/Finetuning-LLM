from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.col("oi.product_id").isin([2, 7, 8]))
    .select(
        F.col("oi.unit_price"),
        F.col("oi.quantity"),
        F.col("o.store_id"),
        F.col("p.stock_qty"),
        F.col("p.cost"),
    )
    .orderBy(F.col("quantity").desc())
)
