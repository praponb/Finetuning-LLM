from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("s.end_date"),
        F.col("c.email"),
        F.col("c.signup_date"),
        F.col("c.is_active"),
        F.length(F.col("c.country")).alias("country_length"),
    )
)
