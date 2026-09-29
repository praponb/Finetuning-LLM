from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id") == 18)
    .select("amount", F.lit("order").alias("source"))
    .union(
        df_subscriptions
        .select("monthly_fee", F.lit("subscription").alias("source"))
    )
    .distinct()
)
