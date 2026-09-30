from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["FROZEN", "OPEN"]) & (F.col("opened_date") <= "2024-10-19"))
    .groupBy("status")
    .agg(F.count(F.when(F.col("account_type") == "CREDIT", 1)).alias("credit_count"))
    .filter(F.col("credit_count") >= 2)
    .orderBy("status")
)
