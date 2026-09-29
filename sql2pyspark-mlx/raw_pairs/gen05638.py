from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .groupBy("account_id")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "left")
    .filter(F.col("as.amount_range") >= 28)
    .select(
        F.col("a.id"),
        F.col("a.account_type"),
        F.col("a.balance"),
        F.col("as.amount_range"),
    )
    .orderBy("amount_range", "account_type")
)
