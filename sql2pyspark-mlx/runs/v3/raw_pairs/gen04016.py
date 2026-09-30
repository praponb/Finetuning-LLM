from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("t.txn_type") != "CREDIT")
    .groupBy(
        F.when(F.col("a.status").isin(["OPEN", "CLOSED"]), "core").otherwise("other").alias("status_group"),
    )
    .agg(
        F.sum(F.when(F.col("t.merchant") == "Lazada", F.col("t.amount")).otherwise(0)).alias("lazada_amount"),
        F.round(((100 * F.sum(F.when(F.col("t.txn_type") == "REFUND", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refund"),
    )
)
