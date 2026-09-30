from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .groupBy(F.col("o.channel"))
    .agg(F.countDistinct(F.col("p.amount")).alias("unique_statuss"))
    .orderBy("unique_statuss", "channel")
    .limit(5)
)
