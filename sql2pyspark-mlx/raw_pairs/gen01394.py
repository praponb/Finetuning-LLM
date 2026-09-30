from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter((F.col("a.account_type") == "CREDIT") & F.col("a.customer_id").isNotNull())
    .select(
        F.col("a.customer_id"),
        F.col("t.txn_type"),
        F.col("t.amount"),
        F.col("t.merchant"),
        F.initcap(F.trim(F.col("a.status"))).alias("status_title"),
    )
)
