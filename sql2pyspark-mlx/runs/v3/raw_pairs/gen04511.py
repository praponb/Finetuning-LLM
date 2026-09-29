from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .select(
        F.col("a.status"),
        F.col("a.opened_date"),
        F.col("t.merchant"),
        F.col("t.is_flagged"),
        F.col("t.account_id"),
    )
    .orderBy("opened_date", "merchant")
)
