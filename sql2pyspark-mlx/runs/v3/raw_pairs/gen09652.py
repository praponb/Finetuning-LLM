from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.plan").isin(["basic", "enterprise"]) & (F.col("s.plan") == "standard"))
    .select(
        F.col("s.monthly_fee"),
        F.col("s.plan"),
        F.col("s.end_date"),
        F.col("c.age"),
        F.col("c.signup_date"),
        F.col("c.email"),
    )
)
