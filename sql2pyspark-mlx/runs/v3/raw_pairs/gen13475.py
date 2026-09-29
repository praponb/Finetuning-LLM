from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") >= 22000) | F.col("opened_date").between("2023-09-01", "2024-01-01"))
    .groupBy("status", F.quarter("opened_date").alias("opened_quarter"))
    .agg(F.min("balance").alias("min_balance"))
    .orderBy(F.col("opened_quarter").desc())
)
