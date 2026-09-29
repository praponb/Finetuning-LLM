from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "outer")
    .filter(F.length(F.col("c.email")) < 10)
    .select(
        F.col("s.end_date"),
        F.col("s.customer_id"),
        F.col("s.plan"),
        F.col("c.email"),
        F.col("c.signup_date"),
        F.col("c.credit_limit"),
    )
    .orderBy("end_date")
)
