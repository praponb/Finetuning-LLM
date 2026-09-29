from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("amount") >= 2000).alias("t"), F.col("a.id") == F.col("t.account_id"), "left_anti")
    .filter(F.col("a.customer_id").isin([15, 26, 40]))
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("a.account_type"),
        F.col("a.status"),
    )
)
