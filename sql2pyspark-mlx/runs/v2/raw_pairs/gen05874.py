from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .groupBy(F.col("a.status"))
    .agg(
        F.countDistinct(F.col("t.amount")).alias("unique_accounts"),
        F.avg(F.col("t.amount")).alias("avg_amount"),
    )
)
