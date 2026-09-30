from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("a.balance") >= 30000)
    .groupBy(F.col("a.status"))
    .agg(
        F.countDistinct(F.col("t.amount")).alias("unique_accounts"),
        F.sum(F.col("t.amount")).alias("total_amount"),
    )
)
