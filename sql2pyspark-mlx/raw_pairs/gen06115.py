from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sum(F.when(F.col("txn_type") == "CREDIT", F.col("amount")).otherwise(0)).alias("credit_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "inner")
    .filter(F.col("tba.avg_amount") >= 44)
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("a.balance"),
        F.col("tba.avg_amount"),
        F.col("tba.credit_amount"),
    )
)
