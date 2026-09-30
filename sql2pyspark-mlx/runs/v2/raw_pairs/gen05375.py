from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left")
    .filter(F.col("oi.order_id").isin([3, 37]))
    .groupBy(F.col("p.brand"))
    .agg(
        F.count(F.col("oi.id")).alias("order_item_count"),
        F.sum(F.col("oi.unit_price")).alias("total_unit_price"),
    )
    .orderBy("total_unit_price")
)
