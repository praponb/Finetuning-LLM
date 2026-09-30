from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(~F.col("t.merchant").isin(["Lazada", "Shell", "Amazon"]) & F.col("a.customer_id").isin([1, 11, 17, 22]))
    .select(
        F.col("t.id"),
        F.col("t.amount"),
        F.col("t.txn_type"),
        F.col("t.merchant"),
        F.col("a.customer_id"),
        F.col("a.status"),
        F.col("t.amount").cast("int").alias("amount_int"),
    )
)
