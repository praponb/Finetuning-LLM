from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "left")
    .select(
        F.col("subscriptions.id"),
        F.col("subscriptions.customer_id"),
        F.col("subscriptions.end_date"),
        F.col("subscriptions.status"),
        F.col("customers.credit_limit"),
    )
    .orderBy(F.col("id").desc())
    .limit(5)
)
