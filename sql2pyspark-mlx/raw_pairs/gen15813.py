from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("o.discount"),
        F.col("o.order_date"),
        F.col("p.category"),
    )
)
