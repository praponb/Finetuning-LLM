from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left")
    .filter(F.col("oi.order_id") == 12)
    .groupBy(F.col("p.id"), F.col("p.brand"))
    .agg(
        F.count(F.col("oi.id")).alias("order_item_count"),
        F.avg(F.col("oi.unit_price")).alias("avg_unit_price"),
        F.avg(F.col("oi.quantity")).alias("avg_quantity"),
    )
)
