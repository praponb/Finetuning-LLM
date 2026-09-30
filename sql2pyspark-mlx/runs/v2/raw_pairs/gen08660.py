from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select(F.col("s.monthly_fee"), F.col("c.signup_date"))
    .orderBy(F.col("monthly_fee").desc())
)
