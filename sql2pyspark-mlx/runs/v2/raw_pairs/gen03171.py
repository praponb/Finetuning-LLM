from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("account_id").isin([27, 39])).alias("t"), F.col("a.id") == F.col("t.account_id"), "left_anti")
    .select(
        F.col("a.id"),
        F.col("a.account_type"),
        F.col("a.opened_date"),
        F.col("a.status"),
    )
    .orderBy(F.col("id").desc())
    .limit(3)
)
