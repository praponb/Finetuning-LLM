from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .filter(F.col("shipped_date").between("2023-04-07", "2024-08-01"))
    .groupBy("order_id")
    .agg(F.max("cost").alias("max_cost"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .select(F.col("o.customer_id"), F.col("o.amount"), F.col("os.max_cost"))
    .orderBy(F.col("customer_id").desc(), "amount")
)
