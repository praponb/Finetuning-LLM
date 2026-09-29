from pyspark.sql import functions as F

recent_accounts = df_accounts.filter(F.col("balance") <= 61000)

summary = (
    recent_accounts
    .groupBy("account_type")
    .agg(F.countDistinct("balance").alias("unique_account_types"))
)

result = (
    summary
    .filter(F.col("unique_account_types") > 6)
    .orderBy("account_type")
    .limit(20)
)
