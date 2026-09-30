from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.col("txn_time") < "2025-05-01")
    .groupBy("account_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Grab", 1).otherwise(0))) / F.count("*")), 1).alias("pct_grab"),
        F.countDistinct("amount").alias("unique_txn_types"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "left")
    .filter(F.col("as.pct_grab") > 28)
    .select(F.col("a.account_type"), F.col("as.pct_grab"), F.col("as.unique_txn_types"))
)
