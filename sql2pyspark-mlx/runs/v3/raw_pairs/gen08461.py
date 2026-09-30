from pyspark.sql import functions as F

customer_stats = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(F.countDistinct("monthly_fee").alias("unique_customers"))
)

result = (
    df_customers.alias("customers")
    .join(customer_stats.alias("customer_stats"), F.col("customers.id") == F.col("customer_stats.customer_id"), "left")
    .select(
        F.col("customers.id"),
        F.col("customers.is_active"),
        F.col("customer_stats.unique_customers"),
    )
)
