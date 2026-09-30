from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.month("opened_date").isin([8, 9, 10]))
    .select("status", F.lit("account").alias("source"))
    .unionByName(
        df_subscriptions
        .select("status", F.lit("subscription").alias("source"))
    )
    .distinct()
    .orderBy("source")
)
