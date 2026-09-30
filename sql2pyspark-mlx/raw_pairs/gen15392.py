from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .filter((F.col("o.amount") <= 1120) | (F.col("oi.order_id") == 29))
    .select(
        F.col("oi.quantity"),
        F.col("oi.product_id"),
        F.col("o.shipping_fee"),
        F.col("p.brand"),
    )
    .orderBy(F.col("quantity").desc())
)
