from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("t.merchant").isin(["Grab", "Starbucks", "Amazon"]) & (F.col("t.account_id") == 34))
    .select(
        F.col("a.status"),
        F.col("t.txn_time"),
        F.col("t.txn_type"),
        F.col("t.account_id"),
        F.coalesce(F.col("a.balance"), F.lit(0)).alias("balance_filled"),
    )
    .orderBy("txn_type")
)
