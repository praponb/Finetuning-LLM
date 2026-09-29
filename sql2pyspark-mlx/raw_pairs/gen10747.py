from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.filter(F.col("start_date") <= "2024-03-20").alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_anti")
    .select(F.col("c.id"), F.col("c.credit_limit"))
)
