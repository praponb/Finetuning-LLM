from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.category"))
    .agg(
        F.countDistinct(F.col("oi.unit_price")).alias("unique_orders"),
        F.count("*").alias("num_order_items"),
    )
    .filter(F.col("unique_orders") >= 4)
    .orderBy(F.col("num_order_items").desc())
)
