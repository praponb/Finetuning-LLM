from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") >= 1210)
    .select("status", F.lit("payment").alias("source"))
    .unionByName(
        df_subscriptions
        .filter(F.month("end_date").isin([4, 5, 12]))
        .select("status", F.lit("subscription").alias("source"))
    )
    .distinct()
    .orderBy(F.col("status").desc())
)
