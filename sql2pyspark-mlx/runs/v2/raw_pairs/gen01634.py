from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.col("txn_type") == "CREDIT")
    .groupBy("account_id")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.max("amount").alias("max_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("as.avg_amount"),
        F.col("as.max_amount"),
    )
    .orderBy(F.col("id").desc())
)
