from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("status") == "FROZEN") & (F.col("account_type") == "LOAN"))
    .groupBy(
        F.date_trunc("MONTH", "opened_date").alias("opened_month"),
        F.when(F.col("balance") >= 87000, "high").when(F.col("balance") >= 11000, "mid").otherwise("low").alias("balance_bucket"),
        "account_type",
    )
    .agg(
        F.sum(F.when(F.col("account_type") == "SAVINGS", F.col("balance")).otherwise(0)).alias("savings_balance"),
        F.max("balance").alias("max_balance"),
    )
    .orderBy("savings_balance")
)
