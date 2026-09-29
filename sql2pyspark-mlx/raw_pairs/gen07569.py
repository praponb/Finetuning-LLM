from pyspark.sql import functions as F

filtered_accounts = (
    df_accounts
    .filter(F.col("customer_id").isNull() & ((F.col("status") == "OPEN") | (F.col("account_type") == "LOAN")))
)

accounts_stats = (
    filtered_accounts
    .groupBy("account_type")
    .agg(F.count(F.when(F.col("account_type") == "SAVINGS", 1)).alias("savings_count"))
)

result = accounts_stats.orderBy("account_type")
