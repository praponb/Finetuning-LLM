from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.filter(F.col("customer_id").isin([6, 8, 18, 21])).alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_semi")
    .select(F.col("c.id"), F.col("c.credit_limit"), F.col("c.age"), F.col("c.name"))
)
