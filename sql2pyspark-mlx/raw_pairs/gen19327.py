from pyspark.sql import functions as F

result = (
    df_transactions
    .alias("s")
    .join(df_accounts.alias("p"), F.col("s.account_id") == F.col("p.id"), "left")
    .cube(F.col("p.status").alias("status"), F.col("s.merchant").alias("merchant"))
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("s.amount")), 2).alias("total_amount"),
    )
    .orderBy(F.col("merchant").desc_nulls_last())
)
