from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_payments.filter(F.to_date("paid_at") < "2023-11-25").alias("payments"), F.col("orders.id") == F.col("payments.order_id"), "left_anti")
    .select(F.col("orders.id"), F.col("orders.status"))
    .orderBy(F.col("id").desc())
    .limit(10)
)
