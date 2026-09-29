from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("amount") <= 1100).alias("t"), F.col("a.id") == F.col("t.account_id"), "left_anti")
    .filter(F.col("a.status") == "OPEN")
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("a.customer_id"),
        F.col("a.opened_date"),
    )
    .orderBy(F.col("status").desc())
)
