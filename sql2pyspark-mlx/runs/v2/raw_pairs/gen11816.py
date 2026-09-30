from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "inner")
    .select(
        F.col("transactions.merchant"),
        F.col("transactions.is_flagged"),
        F.col("accounts.opened_date"),
    )
    .orderBy(F.col("opened_date").desc())
)
