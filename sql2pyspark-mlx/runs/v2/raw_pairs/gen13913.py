from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .select(F.col("c.age"), F.col("s.customer_id"))
)
