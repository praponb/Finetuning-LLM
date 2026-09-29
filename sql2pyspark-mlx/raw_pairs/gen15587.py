from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("subscriptions.customer_id"), "inner")
    .select(
        F.col("subscriptions.id"),
        F.col("subscriptions.monthly_fee"),
        F.col("subscriptions.status"),
        F.col("customers.country"),
        F.col("customers.segment"),
        F.col("customers.city"),
    )
    .orderBy(F.col("monthly_fee").desc())
)
