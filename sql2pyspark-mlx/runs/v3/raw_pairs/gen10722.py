from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.filter(F.col("signup_date") >= "2024-10-01").alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "left_semi")
    .filter(F.col("subscriptions.end_date") > "2024-11-12")
    .select(
        F.col("subscriptions.id"),
        F.col("subscriptions.plan"),
        F.col("subscriptions.customer_id"),
    )
)
