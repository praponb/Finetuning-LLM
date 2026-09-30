from pyspark.sql import functions as F

result = (
    df_subscriptions
    .rollup("customer_id", "plan")
    .agg(
        F.min("start_date").alias("first_start_date"),
        F.count("*").alias("subscription_count"),
    )
    .orderBy("customer_id")
)
