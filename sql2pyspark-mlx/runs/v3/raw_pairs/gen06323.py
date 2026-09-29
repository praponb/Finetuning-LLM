from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_subscriptions.alias("subscriptions"), F.col("subscriptions.customer_id") == F.col("customers.id"), "left")
    .filter(~F.col("customers.is_active"))
    .select(
        F.col("customers.email"),
        F.col("customers.segment"),
        F.col("subscriptions.plan"),
        F.col("subscriptions.monthly_fee"),
        F.col("subscriptions.end_date"),
    )
)
