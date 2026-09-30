from pyspark.sql import functions as F

recent_accounts = (
    df_accounts
    .filter(F.col("status").isin(["FROZEN", "OPEN", "CLOSED"]) & F.month("opened_date").isin([7, 8, 12]) & (F.col("opened_date") < "2024-03-01"))
)

accounts_stats = (
    recent_accounts
    .groupBy("customer_id")
    .agg(F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_count"))
)

result = accounts_stats.orderBy(F.col("frozen_count").desc())
