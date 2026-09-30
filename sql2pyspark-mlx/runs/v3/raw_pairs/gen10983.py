from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.customer_id").isNull())
    .select(F.col("o.customer_id"), F.col("p.status"), F.col("p.order_id"))
    .orderBy(F.col("customer_id").desc())
)
