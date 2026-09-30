from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        "status",
        F.when(F.col("balance") >= 81000, "PREMIUM").otherwise("STANDARD").alias("balance_tier"),
        F.date_trunc("MONTH", "opened_date").alias("activity_month"),
    )
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.round(F.avg("balance"), 2).alias("avg_balance_2"),
        F.round(F.avg("balance"), 2).alias("avg_balance_3"),
        F.sum(F.when(F.col("account_type") == "SAVINGS", F.col("balance")).otherwise(0)).alias("savings_balance"),
        F.sum(F.when(F.col("status") == "FROZEN", F.col("balance"))).alias("frozen_total_balance"),
    )
)
