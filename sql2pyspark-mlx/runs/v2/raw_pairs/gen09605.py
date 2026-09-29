from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "inner")
    .groupBy(
        F.month(F.col("accounts.opened_date")).alias("opened_month"),
        F.col("accounts.account_type"),
    )
    .agg(
        F.round(F.avg(F.col("transactions.amount")), 2).alias("avg_amount"),
        F.max(F.col("transactions.amount")).alias("max_amount"),
    )
    .filter(F.col("avg_amount") > 22)
)
