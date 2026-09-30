from pyspark.sql import functions as F

result = (
    df_transactions
    .alias("a")
    .join(df_accounts.alias("p"), F.col("a.account_id") == F.col("p.id"), "inner")
    .groupBy(
        F.col("p.account_type").alias("account_type"),
        F.col("p.status").alias("status"),
        F.col("a.merchant").alias("merchant"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("a.amount")).alias("min_amount"),
    )
)
