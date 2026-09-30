from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "outer")
    .filter(F.size(F.col("p.tags")) > 2)
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("oi.quantity"),
        F.col("oi.order_id"),
        F.col("p.price"),
        F.col("p.cost"),
        (F.col("p.price") * F.col("p.cost")).alias("margin"),
    )
)
