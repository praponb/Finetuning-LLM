from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("merchant") == "Grab").alias("t"), F.col("a.id") == F.col("t.account_id"), "left_anti")
    .filter(F.col("a.account_type") != "CHECKING")
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("a.account_type"),
        F.col("a.balance"),
    )
    .orderBy("account_type", F.col("status").desc())
)
