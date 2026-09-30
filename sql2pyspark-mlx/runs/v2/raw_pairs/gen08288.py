from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "left")
    .filter(F.col("s.city") != "Tokyo")
    .groupBy(F.col("o.status"))
    .agg(F.count("*").alias("payment_count"))
    .orderBy(F.col("payment_count").desc(), "status")
    .limit(5)
)
