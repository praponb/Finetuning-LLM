from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "left")
    .select(F.col("o.status"), F.col("p.order_id"))
    .orderBy("order_id")
)
