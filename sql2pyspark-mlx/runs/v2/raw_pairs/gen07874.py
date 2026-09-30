from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .groupBy("order_id")
    .agg(F.max("cost").alias("max_cost"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .select(F.col("o.discount"), F.col("o.amount"), F.col("os.max_cost"))
)
