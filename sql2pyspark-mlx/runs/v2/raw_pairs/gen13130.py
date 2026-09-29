from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").between(14, 40))
    .select("status", F.lit("order").alias("source"))
    .unionByName(
        df_subscriptions
        .filter(F.col("customer_id").isin([8, 25, 36]))
        .select("status", F.lit("subscription").alias("source"))
    )
    .orderBy("status")
)
