from pyspark.sql import functions as F

filtered_accounts = df_accounts.filter(F.col("opened_date").between("2024-12-01", "2025-10-03"))

accounts_stats = (
    filtered_accounts
    .groupBy("status", "customer_id")
    .agg(F.max("balance").alias("max_balance"))
)

result = (
    accounts_stats
    .select("status", "customer_id", "max_balance")
    .orderBy(F.col("customer_id").desc(), "status")
    .limit(1)
)
