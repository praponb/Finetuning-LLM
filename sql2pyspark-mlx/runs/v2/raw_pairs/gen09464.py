from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(F.col("is_flagged"))
    .groupBy("account_id")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.count(F.when(F.col("txn_type") == "DEBIT", 1)).alias("debit_count"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("a.account_type"),
        F.col("at.amount_range"),
        F.col("at.debit_count"),
    )
)
