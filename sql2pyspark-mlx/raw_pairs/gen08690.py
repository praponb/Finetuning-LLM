from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sum("amount").alias("total_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "left")
    .filter(F.col("as.avg_amount") >= 23)
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("a.account_type"),
        F.col("as.avg_amount"),
        F.col("as.total_amount"),
    )
    .orderBy(F.col("account_type").desc())
)
