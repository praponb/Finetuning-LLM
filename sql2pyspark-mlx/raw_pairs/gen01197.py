from pyspark.sql import functions as F

result = (
    df_transactions
    .alias("a")
    .join(df_accounts.alias("d"), F.col("a.account_id") == F.col("d.id"), "left")
    .rollup(F.col("d.status").alias("status"), F.col("a.merchant").alias("merchant"))
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("a.amount")).alias("min_amount"),
        F.grouping_id().alias("gid"),
    )
)
