from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(~F.col("a.account_type").isin(["SAVINGS", "CREDIT"]) & (F.col("t.amount") <= 2500))
    .select(
        F.col("a.account_type"),
        F.col("a.customer_id"),
        F.col("a.status"),
        F.col("t.account_id"),
    )
)
