from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        "status",
        F.date_format("opened_date", "yyyy-MM").alias("opened_month"),
        F.when(F.col("balance") >= 83000, "high").when(F.col("balance") >= 8000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("account_type") == "SAVINGS", F.col("balance")).otherwise(0)).alias("savings_balance"),
        F.count(F.when(F.col("account_type") == "LOAN", 1)).alias("loan_count"),
        F.countDistinct("balance").alias("unique_statuss"),
    )
    .filter(F.col("unique_statuss") >= 2)
    .drop("unique_statuss")
    .orderBy("savings_balance")
)
