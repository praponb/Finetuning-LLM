from pyspark.sql import functions as F

recent_accounts = (
    df_accounts
    .filter((F.col("opened_date") <= "2025-07-01") & (F.col("account_type") == "SAVINGS"))
)

accounts_stats = (
    recent_accounts
    .groupBy("status")
    .agg(F.count("*").alias("total_count"))
)

result = (
    accounts_stats
    .select("status", "total_count")
    .orderBy("total_count")
)
