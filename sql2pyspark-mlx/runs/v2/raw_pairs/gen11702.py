from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "inner")
    .groupBy(F.col("orders.customer_id"))
    .agg(
        F.count(F.col("order_items.unit_price")).alias("unit_price_count"),
        F.avg(F.col("order_items.unit_price")).alias("avg_unit_price"),
    )
    .orderBy(F.col("avg_unit_price").desc(), F.col("customer_id").desc())
    .limit(5)
)
