from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") <= 16000) & F.col("account_type").isin(["LOAN", "CREDIT", "SAVINGS"]))
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.max("opened_date").alias("latest_opened_date"),
        F.sum("balance").alias("total_balance"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
    )
    .orderBy(F.col("latest_opened_date").desc())
)
