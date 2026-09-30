from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("subscriptions.customer_id"), "inner")
    .select(
        F.col("subscriptions.id"),
        F.col("subscriptions.customer_id"),
        F.col("subscriptions.status"),
        F.col("subscriptions.start_date"),
        F.col("customers.email"),
        F.col("customers.city"),
        F.col("customers.signup_date"),
    )
)
