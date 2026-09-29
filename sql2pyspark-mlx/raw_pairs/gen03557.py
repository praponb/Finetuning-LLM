from pyspark.sql import functions as F

result = (
    df_transactions
    .alias("s")
    .join(df_accounts.alias("d"), F.col("s.account_id") == F.col("d.id"), "inner")
    .cube(F.col("d.status").alias("status"), F.col("s.merchant").alias("merchant"))
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("s.amount")).alias("max_amount"),
    )
)
