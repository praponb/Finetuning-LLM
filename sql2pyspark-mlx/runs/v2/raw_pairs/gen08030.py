from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("products.id") == F.col("order_items.product_id"), "inner")
    .groupBy(F.col("products.brand"))
    .agg(
        F.count(F.col("order_items.quantity")).alias("quantity_count"),
        F.round(F.avg(F.col("order_items.quantity")), 2).alias("avg_quantity"),
    )
    .orderBy("quantity_count")
)
