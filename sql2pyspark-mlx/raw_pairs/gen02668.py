from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .filter(F.col("oi.order_id") == 23)
    .groupBy(F.col("o.channel"))
    .agg(
        F.countDistinct(F.col("oi.quantity")).alias("unique_orders"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("row_count").desc(), "channel")
    .limit(10)
)
