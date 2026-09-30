from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(~F.col("txn_type").isin(["FEE", "REFUND", "DEBIT"]))
    .groupBy("account_id")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "left")
    .filter(F.col("tba.total_amount") >= 9000)
    .select(
        F.col("a.id"),
        F.col("a.account_type"),
        F.col("a.balance"),
        F.col("tba.total_amount"),
        F.col("tba.stddev_amount"),
    )
)
