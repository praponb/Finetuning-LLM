from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") > 42000) & (F.col("status").isin(["FROZEN", "OPEN"]) | (F.col("account_type") == "SAVINGS")))
    .groupBy(
        F.date_trunc("MONTH", "opened_date").alias("opened_month"),
        F.when(F.col("balance") >= 60000, "high").when(F.col("balance") >= 34000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(F.count(F.when(F.col("account_type") == "SAVINGS", 1)).alias("savings_count"))
    .filter(F.col("savings_count") > 4)
    .orderBy("opened_month", "balance_bucket")
    .limit(1)
)
