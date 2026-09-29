from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("account_id").isin([18, 26, 37])).alias("t"), F.col("a.id") == F.col("t.account_id"), "left_anti")
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("a.balance"),
        F.col("a.status"),
    )
    .orderBy(F.col("balance").desc())
)
