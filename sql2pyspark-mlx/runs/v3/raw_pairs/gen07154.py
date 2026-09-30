from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.col("amount") >= 3300)
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("txn_type") == "CREDIT", F.col("amount")).otherwise(0)).alias("credit_amount"),
        F.avg("amount").alias("avg_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("A"), F.col("a.id") == F.col("A.account_id"), "inner")
    .filter(F.col("A.credit_amount") >= 47)
    .select(F.col("a.balance"), F.col("A.credit_amount"), F.col("A.avg_amount"))
    .orderBy(F.col("balance").desc())
)
