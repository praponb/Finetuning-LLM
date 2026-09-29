from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.countDistinct("amount").alias("unique_txn_types"),
        F.sum(F.when(F.col("merchant") == "Amazon", F.col("amount")).otherwise(0)).alias("amazon_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("t.amazon_amount") >= 34)
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("t.unique_txn_types"),
        F.col("t.amazon_amount"),
    )
    .orderBy(F.col("customer_id").desc())
)
