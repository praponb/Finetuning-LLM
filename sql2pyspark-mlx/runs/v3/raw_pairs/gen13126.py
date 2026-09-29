from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("o.status").isin(["COMPLETED", "SHIPPED", "RETURNED"]))
    .groupBy(F.col("o.customer_id"))
    .agg(F.avg(F.col("oi.quantity")).alias("avg_quantity"))
    .orderBy(F.col("customer_id").desc())
)
