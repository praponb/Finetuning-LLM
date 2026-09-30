from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "left")
    .filter(F.col("o.shipping_fee") <= 20)
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("o.discount"),
        F.col("p.price"),
        F.col("p.product_name"),
    )
    .orderBy("discount", "id")
    .limit(10)
)
