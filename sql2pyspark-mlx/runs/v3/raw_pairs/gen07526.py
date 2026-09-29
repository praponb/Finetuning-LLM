from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(~F.col("a.status").isin(["CLOSED", "OPEN"]) & (F.col("a.account_type") == "CHECKING"))
    .select(
        F.col("a.status"),
        F.col("a.account_type"),
        F.col("t.merchant"),
        F.col("t.amount"),
    )
    .orderBy("account_type")
)
