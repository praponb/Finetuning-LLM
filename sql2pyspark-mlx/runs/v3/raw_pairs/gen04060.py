from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_payments.alias("payments"), F.col("orders.id") == F.col("payments.order_id"), "left_anti")
    .select(F.col("orders.id"), F.col("orders.amount"), F.col("orders.customer_id"))
    .orderBy(F.col("amount").desc(), F.col("id").desc())
    .limit(3)
)
