from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .groupBy(F.col("c.city"))
    .agg(F.max(F.col("p.amount")).alias("max_amount"))
    .filter(F.col("max_amount") > 640)
    .orderBy(F.col("city").desc())
    .limit(5)
)
