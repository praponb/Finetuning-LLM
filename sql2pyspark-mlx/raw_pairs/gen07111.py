from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") <= 62000) & F.col("opened_date").between("2024-07-10", "2024-12-01"))
    .groupBy("customer_id")
    .agg(F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_count"))
)
