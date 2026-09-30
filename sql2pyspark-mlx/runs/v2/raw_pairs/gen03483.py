from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "left")
    .groupBy(F.col("accounts.status"), F.col("accounts.account_type"))
    .agg(
        F.round(F.avg(F.col("transactions.amount")), 2).alias("avg_amount"),
        F.countDistinct(F.col("transactions.amount")).alias("unique_accounts"),
    )
    .orderBy(F.col("account_type").desc())
)
