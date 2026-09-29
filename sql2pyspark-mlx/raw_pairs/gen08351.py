from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "inner")
    .groupBy(F.col("customers.city"))
    .agg(
        F.min(F.col("subscriptions.end_date")).alias("first_end_date"),
        F.count(F.col("subscriptions.monthly_fee")).alias("monthly_fee_count"),
    )
)
