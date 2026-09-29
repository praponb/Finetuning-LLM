from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") != "basic")
    .select(F.col("monthly_fee").alias("value"), F.lit("subscription").alias("source"))
    .unionByName(
        df_orders
        .select(F.col("amount").alias("value"), F.lit("order").alias("source"))
    )
)
