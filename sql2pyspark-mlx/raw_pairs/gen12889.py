from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .groupBy(F.col("p.category"))
    .agg(
        F.min(F.col("oi.quantity")).alias("min_quantity"),
        F.sum(F.col("oi.quantity")).alias("total_quantity"),
    )
)
