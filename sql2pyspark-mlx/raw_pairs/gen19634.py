from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left")
    .select(F.col("c.credit_limit"), F.col("s.monthly_fee"), F.col("s.end_date"))
    .orderBy(F.col("credit_limit").desc())
)
