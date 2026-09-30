from pyspark.sql import functions as F

by_dimension = (
    df_transactions
    .alias("s")
    .join(df_accounts.alias("d"), F.col("s.account_id") == F.col("d.id"), "inner")
    .rollup(
        F.col("d.status").alias("status"),
        F.col("d.account_type").alias("account_type"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("s.amount")).alias("max_amount"),
    )
)

result = by_dimension.select("status", "cnt", "max_amount")
