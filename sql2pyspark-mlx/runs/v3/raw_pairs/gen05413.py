from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .groupBy(F.month(F.col("c.signup_date")).alias("signup_month"), F.col("c.country"))
    .agg(
        F.sum(F.when(F.col("s.plan") == "premium", 1).otherwise(0)).alias("premium_count"),
    )
    .orderBy(F.col("country").desc())
)
