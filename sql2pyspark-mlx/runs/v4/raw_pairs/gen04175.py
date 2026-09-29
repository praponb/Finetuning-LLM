from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.amount") > 1110)
    .groupBy(F.col("p.category"))
    .agg(
        F.count("*").alias("total_count"),
        F.count("*").alias("order_item_count"),
    )
    .orderBy("category")
)
