from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.store_id") == 14)
    .select(F.col("p.amount"), F.col("p.status"), F.col("p.method"), F.col("o.channel"))
    .orderBy("method", "channel")
)
