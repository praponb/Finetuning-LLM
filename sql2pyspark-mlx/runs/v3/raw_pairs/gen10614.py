from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("a.status").isin(["CLOSED", "FROZEN"]) & (F.col("a.balance") < 51000))
    .select(F.col("a.balance"), F.col("t.merchant"))
    .orderBy("balance", "merchant")
)
