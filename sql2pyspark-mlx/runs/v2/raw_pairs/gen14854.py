from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "right")
    .filter((F.col("t.merchant") == "Shell") & F.col("a.account_type").isin(["SAVINGS", "CHECKING"]))
    .select(
        F.col("t.id"),
        F.col("t.merchant"),
        F.col("t.account_id"),
        F.col("t.amount"),
        F.col("a.status"),
        F.col("a.customer_id"),
        F.col("a.balance"),
        F.abs(F.col("a.balance")).alias("abs_balance"),
    )
    .orderBy("amount")
)
