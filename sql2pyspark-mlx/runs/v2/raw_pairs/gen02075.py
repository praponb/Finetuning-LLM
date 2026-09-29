from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(45000, 62000))
    .groupBy("status", "account_type")
    .agg(F.count(F.when(F.col("account_type") == "SAVINGS", 1)).alias("savings_count"))
    .filter(F.col("savings_count") < 6)
    .orderBy("account_type", "status")
    .limit(3)
)
