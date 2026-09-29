from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2024-06-24", "2025-04-01") & F.col("opened_date").between("2023-08-25", "2024-04-03") & (F.col("balance") > 46000))
    .agg(F.count("balance").alias("balance_count"))
    .orderBy(F.col("balance_count").desc())
)
