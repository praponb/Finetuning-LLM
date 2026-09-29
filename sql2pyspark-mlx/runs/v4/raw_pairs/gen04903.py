from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "right")
    .filter(F.col("customers.email").isNotNull() | (F.col("customers.city") == "London"))
    .select(
        F.col("subscriptions.start_date"),
        F.col("subscriptions.monthly_fee"),
        F.col("customers.is_active"),
        F.col("customers.country"),
        F.col("customers.signup_date"),
    )
)
