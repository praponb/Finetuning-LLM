from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .select(
        F.col("t.account_id"),
        F.col("t.amount"),
        F.col("a.status"),
        F.col("a.balance"),
        F.lower(F.col("a.account_type")).alias("account_type_lower"),
    )
    .orderBy("account_id")
)
