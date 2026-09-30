from pyspark.sql import functions as F

valid_transactions = df_transactions.filter(F.hour("txn_time").between(6, 14))

summary = (
    valid_transactions
    .groupBy("merchant")
    .agg(F.countDistinct("amount").alias("unique_accounts"))
)

result = (
    summary
    .filter(F.col("unique_accounts") > 8)
    .select("merchant", "unique_accounts")
    .orderBy("merchant")
    .limit(3)
)
