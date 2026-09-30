from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("c.signup_date"),
        F.col("c.credit_limit"),
        F.col("c.name"),
    )
    .orderBy("signup_date", F.col("id").desc())
    .limit(5)
)
