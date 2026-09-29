from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "outer")
    .filter(F.col("s.customer_id").isin([20, 37]))
    .select(F.col("s.id"), F.col("s.customer_id"), F.col("s.plan"), F.col("c.email"))
    .orderBy("plan")
)
