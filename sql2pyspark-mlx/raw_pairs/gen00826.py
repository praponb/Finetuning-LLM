from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.filter(F.to_date("paid_at") >= "2023-05-01").alias("p"), F.col("o.id") == F.col("p.order_id"), "left_anti")
    .filter(F.col("o.customer_id") == 4)
    .select(F.col("o.id"), F.col("o.order_date"), F.col("o.amount"), F.col("o.status"))
    .orderBy(F.col("id").desc())
)
