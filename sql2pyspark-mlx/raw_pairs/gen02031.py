from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.col("oi.order_id").isin([7, 35, 37]))
    .groupBy(F.col("p.id"), F.col("p.product_name"))
    .agg(
        F.count(F.col("oi.id")).alias("order_item_count"),
        F.min(F.col("oi.quantity")).alias("min_quantity"),
    )
    .orderBy("id")
)
