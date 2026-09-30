from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.status").isin(["ACTIVE", "CANCELLED"]) & (F.col("c.segment") == "Consumer"))
    .select(F.col("c.is_active"), F.col("s.status"))
)
