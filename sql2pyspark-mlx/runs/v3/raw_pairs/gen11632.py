from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_subscriptions.alias("subscriptions"), F.col("subscriptions.customer_id") == F.col("customers.id"), "inner")
    .select(
        F.col("customers.name"),
        F.col("customers.country"),
        F.col("customers.city"),
        F.col("subscriptions.status"),
        F.col("subscriptions.monthly_fee"),
        F.col("subscriptions.customer_id"),
    )
    .orderBy(F.col("status").desc())
)
