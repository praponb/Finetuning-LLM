from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .filter(F.col("o.customer_id").isin([1, 6, 23]))
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("p.brand"),
        F.col("o.channel"),
        F.col("o.status"),
    )
    .orderBy("id")
)
