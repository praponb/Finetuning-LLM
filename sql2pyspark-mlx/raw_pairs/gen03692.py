from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.col("merchant") == "Grab")
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("txn_type") == "CREDIT", 1).otherwise(0)).alias("credit_count"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .filter(F.col("as.amount_range") > 39)
    .select(
        F.col("a.id"),
        F.col("a.account_type"),
        F.col("a.balance"),
        F.col("as.credit_count"),
        F.col("as.amount_range"),
    )
    .orderBy(F.col("credit_count").desc(), "account_type", "id")
    .limit(3)
)
