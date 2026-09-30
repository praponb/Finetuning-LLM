from pyspark.sql import functions as F

valid_accounts = df_accounts.filter(F.col("account_type") == "SAVINGS")

accounts_stats = (
    valid_accounts
    .groupBy("account_type")
    .agg(F.max("opened_date").alias("latest_opened_date"))
)

result = (
    accounts_stats
    .select("account_type", "latest_opened_date")
    .orderBy(F.col("latest_opened_date").desc(), "account_type")
    .limit(3)
)
