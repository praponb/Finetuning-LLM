from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") >= 0.37)
    .select("status", F.lit("order").alias("source"))
    .unionByName(
        df_subscriptions
        .select("status", F.lit("subscription").alias("source"))
    )
)
