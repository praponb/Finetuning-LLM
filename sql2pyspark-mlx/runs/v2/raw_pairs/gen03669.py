from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("subscriptions.monthly_fee").between(80, 150))
    .select(
        F.col("subscriptions.id"),
        F.col("subscriptions.customer_id"),
        F.col("subscriptions.start_date"),
        F.col("subscriptions.monthly_fee"),
        F.col("customers.name"),
        F.col("customers.credit_limit"),
    )
    .orderBy(F.col("monthly_fee").desc(), "customer_id")
)
