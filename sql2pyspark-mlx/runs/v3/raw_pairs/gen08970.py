from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("a.account_type") == "SAVINGS")
    .select(
        F.col("a.account_type"),
        F.col("t.is_flagged"),
        F.col("t.amount"),
        F.col("t.account_id"),
    )
    .orderBy("account_id", F.col("amount").desc())
)
