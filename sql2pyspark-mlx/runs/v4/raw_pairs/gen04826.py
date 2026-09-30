from pyspark.sql import functions as F

result = (
    df_orders
    .select("status", F.lit("order").alias("source"))
    .unionByName(
        df_subscriptions
        .filter(F.col("plan") != "basic")
        .select("status", F.lit("subscription").alias("source"))
    )
)
