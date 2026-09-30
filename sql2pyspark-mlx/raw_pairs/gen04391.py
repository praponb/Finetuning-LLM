from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "left_anti")
    .filter(F.col("o.status") == "RETURNED")
    .select(F.col("o.id"), F.col("o.order_date"), F.col("o.customer_id"))
    .orderBy(F.col("customer_id").desc(), "order_date", "id")
    .limit(3)
)
