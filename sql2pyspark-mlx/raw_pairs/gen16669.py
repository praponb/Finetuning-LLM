from pyspark.sql import functions as F

valid_accounts = (
    df_accounts
    .filter((F.col("customer_id") == 11) & ~F.col("status").isin(["CLOSED", "OPEN"]) & (F.col("status") != "OPEN"))
)

accounts_stats = (
    valid_accounts
    .groupBy("account_type")
    .agg(F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_count"))
)

result = accounts_stats.filter(F.col("closed_count") >= 5)
