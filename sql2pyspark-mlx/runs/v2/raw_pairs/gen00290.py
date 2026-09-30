from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .filter(F.col("order_id").isin([1, 4, 30, 33]))
    .groupBy("order_id")
    .agg(F.sum("quantity").alias("total_quantity"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("t"), F.col("o.id") == F.col("t.order_id"), "left")
    .select(F.col("o.amount"), F.col("t.total_quantity"))
    .orderBy(F.col("total_quantity").desc())
)
