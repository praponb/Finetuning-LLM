from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "inner")
    .groupBy(F.col("customers.country"))
    .agg(
        F.max(F.col("subscriptions.end_date")).alias("latest_end_date"),
        F.min(F.col("subscriptions.start_date")).alias("first_start_date"),
    )
)
