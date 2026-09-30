from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("oi.product_id") == F.col("p.id"), "left")
    .filter(F.col("oi.quantity") >= 7)
    .groupBy(F.col("p.id"))
    .agg(
        F.count(F.col("oi.id")).alias("order_item_count"),
        F.sum(F.col("oi.unit_price")).alias("total_unit_price"),
    )
    .filter(F.col("order_item_count") >= 1)
    .orderBy("total_unit_price", "id")
    .limit(1)
)
