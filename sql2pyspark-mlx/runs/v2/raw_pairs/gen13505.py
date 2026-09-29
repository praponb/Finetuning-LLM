from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("o.status"))
    .agg(
        F.count("*").alias("total_count"),
        F.round(F.avg(F.col("oi.unit_price")), 2).alias("avg_unit_price"),
    )
    .orderBy("total_count", "status")
    .limit(5)
)
