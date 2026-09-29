from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.filter(~F.col("account_type").isin(["CHECKING", "LOAN", "SAVINGS"])).alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "left_semi")
    .select(
        F.col("transactions.id"),
        F.col("transactions.is_flagged"),
        F.col("transactions.amount"),
        F.col("transactions.txn_time"),
    )
    .orderBy("id")
)
