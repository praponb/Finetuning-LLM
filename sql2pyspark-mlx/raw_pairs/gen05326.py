from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("merchant") != "Shell")
    .groupBy("account_id")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.countDistinct("amount").alias("unique_merchants"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "left")
    .select(
        F.col("a.id"),
        F.col("a.account_type"),
        F.col("a.customer_id"),
        F.col("tba.total_amount"),
        F.col("tba.unique_merchants"),
    )
    .orderBy("account_type", F.col("total_amount").desc())
)
