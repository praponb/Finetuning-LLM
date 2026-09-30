from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.region"))
    .agg(F.count(F.when(F.col("p.method") == "paypal", 1)).alias("paypal_count"))
    .filter(F.col("paypal_count") >= 3)
    .orderBy("region")
)
