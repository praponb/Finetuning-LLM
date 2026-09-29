from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2024-02-22", "2025-10-01") & (F.col("opened_date") >= "2023-12-01"))
    .agg(F.count("*").alias("account_count"))
)
