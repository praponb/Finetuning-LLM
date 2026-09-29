from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("merchant") == "Shell", 1).otherwise(0)).alias("shell_count"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("A"), F.col("a.id") == F.col("A.account_id"), "inner")
    .filter(F.col("A.shell_count") >= 43)
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("A.shell_count"),
        F.col("A.avg_amount"),
    )
)
