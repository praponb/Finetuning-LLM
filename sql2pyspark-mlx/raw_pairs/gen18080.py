from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.filter(~F.col("city").isin(["London", "New York"])).alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "left_semi")
    .select(
        F.col("subscriptions.id"),
        F.col("subscriptions.end_date"),
        F.col("subscriptions.status"),
    )
)
