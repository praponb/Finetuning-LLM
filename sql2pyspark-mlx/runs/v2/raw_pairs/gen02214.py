from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([12, 16, 23, 27]) & (F.col("balance") < 58000) & F.col("account_type").isin(["SAVINGS", "CREDIT"]))
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.round(((100 * F.sum(F.when(F.col("account_type") == "CREDIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_credit"),
        F.min("balance").alias("min_balance"),
    )
)
