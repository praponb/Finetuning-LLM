from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "right")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.array_contains(F.col("p.tags"), "new"))
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("p.category"),
        F.col("p.product_name"),
        F.col("o.channel"),
    )
)
