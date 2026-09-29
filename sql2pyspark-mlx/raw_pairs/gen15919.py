from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .filter(F.col("o.amount") <= 1040)
    .select(F.col("p.id"), F.col("p.status"), F.col("p.order_id"), F.col("o.amount"))
    .orderBy("order_id")
)
