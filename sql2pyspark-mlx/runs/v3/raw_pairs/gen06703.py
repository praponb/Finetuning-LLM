from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("start_date", F.lit("subscription").alias("source"))
    .union(df_accounts.select("opened_date", F.lit("account").alias("source")))
    .distinct()
)
