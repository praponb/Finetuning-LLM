from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .groupBy(F.col("a.status"))
    .agg(
        F.sum(F.when(F.col("t.txn_type") == "FEE", F.col("t.amount")).otherwise(0)).alias("fee_amount"),
    )
    .orderBy(F.col("fee_amount").desc(), "status")
    .limit(20)
)
