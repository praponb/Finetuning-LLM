from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .filter(F.col("oi.product_id").isNotNull())
    .join(df_products.filter(F.col("product_name").isNotNull()).alias("p"), F.col("oi.product_id") == F.col("p.id"), "left_anti")
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("oi.quantity"),
        F.col("oi.order_id"),
    )
)
