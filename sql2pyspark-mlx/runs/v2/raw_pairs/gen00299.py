from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .select(
        F.col("oi.quantity"),
        F.col("o.channel"),
        F.col("p.cost"),
        F.col("p.product_name"),
    )
    .orderBy(F.col("quantity").desc())
)
