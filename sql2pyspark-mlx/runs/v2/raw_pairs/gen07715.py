from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2023-07-01", "2025-09-01"))
    .groupBy("account_type")
    .agg(F.count("balance").alias("balance_count"))
    .orderBy(F.col("balance_count").desc())
)
