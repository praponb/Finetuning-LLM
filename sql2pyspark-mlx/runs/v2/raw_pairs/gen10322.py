from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .groupBy(F.col("o.status"))
    .agg(
        F.max(F.col("oi.quantity")).alias("max_quantity"),
        F.avg(F.col("oi.quantity")).alias("avg_quantity"),
    )
    .filter(F.col("max_quantity") >= 3.3)
    .orderBy(F.col("status").desc(), F.col("avg_quantity").desc())
)
