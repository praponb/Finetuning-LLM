from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNull())
    .select(F.lit("payment").alias("kind"), "status")
    .unionByName(df_subscriptions.select(F.lit("subscription").alias("kind"), "status"))
    .distinct()
)
