from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("c.country"),
        F.col("c.signup_date"),
        F.col("c.credit_limit"),
        F.col("s.plan"),
    )
)
