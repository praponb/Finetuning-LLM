from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .groupBy(F.col("p.category"))
    .agg(
        F.sum(F.col("oi.quantity")).alias("total_quantity"),
        F.avg(F.col("oi.unit_price")).alias("avg_unit_price"),
    )
)
