from pyspark.sql import functions as F

result = (
    df_payments
    .select(F.lit("payment").alias("source"), "status")
    .unionByName(
        df_subscriptions
        .select(F.lit("subscription").alias("source"), "status")
    )
    .orderBy(F.col("status").desc())
)
