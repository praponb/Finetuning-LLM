from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_anti")
    .filter(F.col("c.credit_limit") > 6000)
    .select(F.col("c.id"), F.col("c.is_active"), F.col("c.country"))
)
