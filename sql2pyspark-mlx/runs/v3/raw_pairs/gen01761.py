from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("start_date")
    .unionByName(df_accounts.select(F.col("opened_date").alias("start_date")))
    .distinct()
)
