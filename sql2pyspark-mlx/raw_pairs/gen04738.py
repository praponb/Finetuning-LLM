from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "right")
    .select(
        F.col("subscriptions.start_date"),
        F.col("subscriptions.plan"),
        F.col("customers.city"),
        F.col("customers.is_active"),
    )
)
