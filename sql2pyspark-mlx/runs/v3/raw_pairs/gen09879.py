from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("a.status").isin(["CLOSED", "FROZEN"]) & ~F.col("t.merchant").isin(["Amazon", "Starbucks"]))
    .select(
        F.col("t.is_flagged"),
        F.col("t.account_id"),
        F.col("t.merchant"),
        F.col("a.account_type"),
    )
    .orderBy("account_type", F.col("merchant").desc())
)
