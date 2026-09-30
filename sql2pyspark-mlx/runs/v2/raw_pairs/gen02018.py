from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left")
    .filter(F.col("c.name").isNull())
    .select(
        F.col("c.signup_date"),
        F.col("c.email"),
        F.col("c.credit_limit"),
        F.col("s.customer_id"),
        F.col("s.end_date"),
    )
)
