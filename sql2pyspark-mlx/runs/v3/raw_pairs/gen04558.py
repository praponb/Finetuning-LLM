from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.filter(F.col("credit_limit") > 8000).alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "left_semi")
    .filter(F.col("subscriptions.monthly_fee") >= 220)
    .select(
        F.col("subscriptions.id"),
        F.col("subscriptions.customer_id"),
        F.col("subscriptions.monthly_fee"),
    )
)
