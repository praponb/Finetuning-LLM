from pyspark.sql import functions as F

customer_stats = (
    df_orders
    .filter(F.col("customer_id").isin([20, 23]))
    .groupBy("customer_id")
    .agg(F.max("shipping_fee").alias("max_shipping_fee"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .select(F.col("c.id"), F.col("c.credit_limit"), F.col("t.max_shipping_fee"))
    .orderBy("max_shipping_fee")
)
