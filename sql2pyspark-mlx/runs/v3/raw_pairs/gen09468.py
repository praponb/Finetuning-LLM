from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([12, 13]))
    .select("status", F.lit("payment").alias("source"))
    .unionByName(
        df_subscriptions
        .filter(F.col("monthly_fee") >= 210)
        .select("status", F.lit("subscription").alias("source"))
    )
    .distinct()
    .orderBy(F.col("source").desc())
)
