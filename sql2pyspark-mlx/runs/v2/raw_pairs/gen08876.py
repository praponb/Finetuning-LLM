from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNull())
    .select("status", F.lit("payment").alias("source"))
    .unionByName(
        df_subscriptions
        .filter(F.col("status") == "CANCELLED")
        .select("status", F.lit("subscription").alias("source"))
    )
    .distinct()
    .orderBy("source")
)
