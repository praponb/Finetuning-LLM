from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .groupBy("order_id")
    .agg(F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .filter(F.col("t.returned_count") >= 9)
    .select(F.col("o.amount"), F.col("t.returned_count"))
    .orderBy("amount")
)
