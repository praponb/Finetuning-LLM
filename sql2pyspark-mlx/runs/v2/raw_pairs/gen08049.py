from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("a.customer_id").isin([15, 27, 35]))
    .select(
        F.col("a.customer_id"),
        F.col("a.account_type"),
        F.col("t.merchant"),
        F.col("t.amount"),
    )
    .orderBy(F.col("amount").desc())
)
