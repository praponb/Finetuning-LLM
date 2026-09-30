from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("t.is_flagged"))
    .select(
        F.col("a.status"),
        F.col("a.account_type"),
        F.col("t.merchant"),
        F.when(F.col("t.merchant") == "Shell", 1).otherwise(0).alias("is_shell"),
    )
    .orderBy("account_type")
)
