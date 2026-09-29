from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "left")
    .filter(F.col("p.method").isNotNull())
    .select(F.col("o.customer_id"), F.col("p.amount"))
)
