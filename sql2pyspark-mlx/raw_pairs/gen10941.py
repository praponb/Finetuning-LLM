from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_subscriptions.alias("subscriptions"), F.col("subscriptions.customer_id") == F.col("customers.id"), "left")
    .select(
        F.col("customers.city"),
        F.col("subscriptions.status"),
        F.col("subscriptions.start_date"),
        F.coalesce(F.col("subscriptions.monthly_fee"), F.lit(0)).alias("monthly_fee_filled"),
    )
)
