from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([11, 15, 32, 39]))
    .select("status", F.lit("payment").alias("source"))
    .unionByName(
        df_subscriptions
        .select("status", F.lit("subscription").alias("source"))
    )
    .orderBy("status")
)
