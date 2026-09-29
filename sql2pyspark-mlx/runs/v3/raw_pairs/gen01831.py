from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("p.product_name").isNull() & (F.col("p.cost") < 550))
    .select(
        F.col("oi.order_id"),
        F.col("oi.quantity"),
        F.col("p.stock_qty"),
        F.col("o.discount"),
        F.col("o.store_id"),
    )
)
