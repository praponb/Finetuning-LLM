from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.filter(F.col("customer_id").isNull()).alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_anti")
    .filter(F.col("c.is_active"))
    .select(F.col("c.id"), F.col("c.credit_limit"))
    .orderBy("id")
)
