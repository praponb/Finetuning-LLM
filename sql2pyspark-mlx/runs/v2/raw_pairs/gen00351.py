from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .filter(F.col("o.status") == "SHIPPED")
    .groupBy(F.col("o.status"))
    .agg(F.max(F.col("p.amount")).alias("max_amount"))
    .orderBy("status")
)
