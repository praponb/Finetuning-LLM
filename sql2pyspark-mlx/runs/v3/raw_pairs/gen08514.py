from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter((F.col("a.status") != "FROZEN") & F.col("a.customer_id").isNotNull())
    .select(
        F.col("a.opened_date"),
        F.col("a.status"),
        F.col("a.balance"),
        F.col("t.txn_time"),
        F.col("t.amount"),
    )
    .orderBy("opened_date")
)
