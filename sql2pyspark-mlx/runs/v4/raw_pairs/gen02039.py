from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.filter(F.col("amount").between(990, 1090)).alias("p"), F.col("o.id") == F.col("p.order_id"), "left_anti")
    .select(F.col("o.id"), F.col("o.status"), F.col("o.customer_id"), F.col("o.amount"))
)
