from pyspark.sql import functions as F

filtered_accounts = (
    df_accounts
    .filter((F.col("opened_date") > "2024-05-12") & ~F.col("account_type").isin(["SAVINGS", "CREDIT", "CHECKING"]))
)

accounts_stats = (
    filtered_accounts
    .groupBy(
        F.when(F.col("balance") >= 81000, "high").when(F.col("balance") >= 15000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.max("opened_date").alias("last_opened_date"),
        F.sum(F.when(F.col("status") == "OPEN", 1).otherwise(0)).alias("open_count"),
    )
)

result = (
    accounts_stats
    .filter(F.col("open_count") > 44)
    .orderBy("last_opened_date")
)
