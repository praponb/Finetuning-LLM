from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(F.col("txn_type") == "REFUND")
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("merchant") == "Shell", 1).otherwise(0)).alias("shell_count"),
        F.count("amount").alias("amount_count"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("t.amount_count") >= 9)
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("a.status"),
        F.col("t.shell_count"),
        F.col("t.amount_count"),
    )
    .orderBy(F.col("amount_count").desc())
)
