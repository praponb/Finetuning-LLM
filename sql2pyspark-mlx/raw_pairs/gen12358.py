from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(F.col("txn_type").isin(["REFUND", "CREDIT"]))
    .groupBy("account_id")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.count(F.when(F.col("txn_type") == "CREDIT", 1)).alias("credit_count"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "inner")
    .filter(F.col("at.credit_count") > 6)
    .select(
        F.col("a.id"),
        F.col("a.account_type"),
        F.col("a.balance"),
        F.col("at.avg_amount"),
        F.col("at.credit_count"),
    )
)
