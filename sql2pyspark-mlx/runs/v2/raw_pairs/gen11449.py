from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("t.txn_type"),
        F.col("a.status"),
        F.col("a.balance"),
        F.col("c.is_active"),
        F.col("c.segment"),
        F.concat_ws("/", F.col("a.status"), F.col("a.account_type")).alias("status_account_type_key"),
    )
    .orderBy(F.col("status").desc())
)
