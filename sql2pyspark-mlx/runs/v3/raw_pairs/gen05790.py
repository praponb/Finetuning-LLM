from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.start_date") >= "2025-09-03")
    .select(F.col("s.end_date"), F.col("c.signup_date"), F.col("c.is_active"))
)
