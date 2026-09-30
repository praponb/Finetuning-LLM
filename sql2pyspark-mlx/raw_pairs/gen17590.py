from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("s.plan").isin(["enterprise", "basic"]))
    .select(
        F.col("s.id"),
        F.col("s.plan"),
        F.col("c.country"),
        F.col("c.signup_date"),
        F.col("c.credit_limit"),
    )
)
