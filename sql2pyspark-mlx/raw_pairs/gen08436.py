from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "left")
    .select(
        F.col("t.account_id"),
        F.col("t.merchant"),
        F.col("a.customer_id"),
        F.col("a.account_type"),
        F.col("a.balance"),
    )
    .orderBy("merchant")
)
