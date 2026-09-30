from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.month("opened_date").isin([3, 11, 12]))
    .select("status", F.lit("account").alias("record_type"))
    .unionByName(
        df_subscriptions
        .filter(F.col("customer_id").isin([15, 22]))
        .select("status", F.lit("subscription").alias("record_type"))
    )
    .distinct()
)
