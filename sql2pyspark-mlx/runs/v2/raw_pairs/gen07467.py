from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.filter(F.col("amount") <= 1060).alias("p"), F.col("o.id") == F.col("p.order_id"), "left_anti")
    .filter(F.year(F.col("o.order_date")) >= 2025)
    .select(F.col("o.id"), F.col("o.channel"), F.col("o.amount"), F.col("o.status"))
    .orderBy("channel")
)
