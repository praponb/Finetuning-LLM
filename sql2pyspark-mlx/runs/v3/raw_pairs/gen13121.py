from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .filter(F.col("oi.order_id").isin([23, 24, 26, 33]))
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("p.stock_qty"),
        F.col("o.channel"),
        F.col("o.customer_id"),
        F.length(F.col("p.category")).alias("category_length"),
    )
    .orderBy("id")
)
