from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("customers.city") == "Singapore")
    .select(
        F.col("subscriptions.id"),
        F.col("subscriptions.monthly_fee"),
        F.col("customers.segment"),
        F.col("customers.credit_limit"),
    )
    .orderBy(F.col("credit_limit").desc())
)
