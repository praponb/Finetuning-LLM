from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.col("txn_type") == "FEE")
    .groupBy("account_id")
    .agg(F.min("amount").alias("min_amount"))
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "left")
    .filter(F.col("as.min_amount") > 1300)
    .select(
        F.col("a.id"),
        F.col("a.opened_date"),
        F.col("a.status"),
        F.col("as.min_amount"),
    )
    .orderBy("status")
)
