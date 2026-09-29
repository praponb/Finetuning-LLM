from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("txn_type").isin(["REFUND", "FEE", "DEBIT"]))
    .groupBy("account_id")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.min("amount").alias("min_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "left")
    .select(F.col("a.customer_id"), F.col("tba.stddev_amount"), F.col("tba.min_amount"))
)
