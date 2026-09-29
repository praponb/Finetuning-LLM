from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.end_date"),
        F.col("s.monthly_fee"),
        F.col("c.email"),
        F.col("c.signup_date"),
        F.col("c.country"),
    )
    .orderBy("monthly_fee", "id")
    .limit(10)
)
