from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id") == 9)
    .select("status", F.lit("account").alias("source"))
    .unionByName(
        df_subscriptions
        .filter(F.col("monthly_fee") < 180)
        .select("status", F.lit("subscription").alias("source"))
    )
)
