from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .groupBy("account_id")
    .agg(F.countDistinct("amount").alias("unique_accounts"))
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "inner")
    .select(F.col("a.id"), F.col("a.customer_id"), F.col("tba.unique_accounts"))
    .orderBy("id")
    .limit(50)
)
