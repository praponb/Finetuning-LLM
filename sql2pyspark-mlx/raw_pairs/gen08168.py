from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_subscriptions.alias("subscriptions"), F.col("subscriptions.customer_id") == F.col("customers.id"), "left")
    .select(
        F.col("customers.segment"),
        F.col("customers.is_active"),
        F.col("subscriptions.customer_id"),
    )
    .orderBy("segment")
)
