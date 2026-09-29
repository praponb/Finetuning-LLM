from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("a.balance") > 24000)
    .select(
        F.col("t.is_flagged"),
        F.col("t.txn_type"),
        F.col("t.merchant"),
        F.col("a.customer_id"),
        F.coalesce(F.col("t.amount"), F.lit(0)).alias("amount_filled"),
    )
)
