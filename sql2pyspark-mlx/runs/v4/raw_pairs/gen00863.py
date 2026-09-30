from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status", F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.sum(F.when(F.col("account_type") == "CREDIT", 1).otherwise(0)).alias("credit_count"),
        F.round(((100 * F.sum(F.when(F.col("account_type") == "SAVINGS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_savings"),
    )
)
