from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("txn_type").isin(["REFUND", "FEE", "DEBIT"]))
    .groupBy("account_id")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.sum(F.when(F.col("merchant") == "Starbucks", F.col("amount")).otherwise(0)).alias("starbucks_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "left")
    .filter(F.col("as.starbucks_amount") > 41)
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("as.amount_range"),
        F.col("as.starbucks_amount"),
    )
    .orderBy(F.col("amount_range").desc(), F.col("id").desc())
)
