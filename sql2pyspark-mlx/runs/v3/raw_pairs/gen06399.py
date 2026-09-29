from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .groupBy(F.year(F.col("o.order_date")).alias("order_year"))
    .agg(
        F.countDistinct(F.col("oi.quantity")).alias("unique_orders"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("order_year").desc())
)
