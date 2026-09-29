from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("t.account_id").isin([10, 16, 24, 33]))
    .select(
        F.col("a.status"),
        F.col("a.account_type"),
        F.col("a.opened_date"),
        F.col("t.merchant"),
        F.col("t.account_id"),
    )
    .orderBy(F.col("opened_date").desc())
)
