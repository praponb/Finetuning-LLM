from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.col("amount").isNotNull())
    .groupBy("account_id")
    .agg(F.count("amount").alias("amount_count"))
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "left")
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("a.opened_date"),
        F.col("as.amount_count"),
    )
    .orderBy(F.col("amount_count").desc())
)
