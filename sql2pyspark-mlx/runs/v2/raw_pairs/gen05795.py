from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~(F.col("account_type").isin(["SAVINGS", "CHECKING"]) & F.col("status").isin(["OPEN", "FROZEN"])))
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        (F.max("balance") - F.min("balance")).alias("balance_range"),
        F.sum(F.when(F.col("account_type") == "CREDIT", 1).otherwise(0)).alias("credit_count"),
        F.avg("balance").alias("avg_balance"),
    )
)
