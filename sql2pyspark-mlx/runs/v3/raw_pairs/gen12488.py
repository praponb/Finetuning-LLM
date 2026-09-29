from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("customers.segment") == "Corporate")
    .select(
        F.col("subscriptions.id"),
        F.col("subscriptions.start_date"),
        F.col("customers.email"),
        F.coalesce(F.col("subscriptions.monthly_fee"), F.lit(0)).alias("monthly_fee_filled"),
    )
    .orderBy("monthly_fee_filled")
)
