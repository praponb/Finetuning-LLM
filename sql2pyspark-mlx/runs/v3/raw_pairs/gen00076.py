from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.hour(F.col("t.txn_time")).between(10, 13) & (F.col("a.customer_id") == 7))
    .select(
        F.col("t.account_id"),
        F.col("a.status"),
        F.col("a.balance"),
        F.col("a.opened_date"),
        F.round(F.col("t.amount"), 0).alias("amount_rounded"),
    )
    .orderBy(F.col("opened_date").desc())
)
