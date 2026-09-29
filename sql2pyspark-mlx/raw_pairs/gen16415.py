from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([5, 14, 23]))
    .select("status", F.lit("payment").alias("origin"))
    .unionByName(
        df_subscriptions
        .filter(F.col("plan") != "premium")
        .select("status", F.lit("subscription").alias("origin"))
    )
    .orderBy("origin")
)
