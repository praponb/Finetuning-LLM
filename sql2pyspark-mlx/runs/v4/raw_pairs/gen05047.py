from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter((F.col("s.plan") == "enterprise") | F.col("c.city").isin(["Berlin", "Bangkok"]))
    .select(
        F.col("s.id"),
        F.col("s.monthly_fee"),
        F.col("s.start_date"),
        F.col("c.signup_date"),
    )
)
