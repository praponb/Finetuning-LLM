from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["CHECKING", "SAVINGS"]) | (F.col("balance") <= 62000))
    .groupBy(
        "account_type",
        F.when(F.col("status").isin(["FROZEN", "OPEN"]), "priority").otherwise("other").alias("status_group"),
        F.date_format("opened_date", "yyyy-MM").alias("opened_month"),
    )
    .agg(
        F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_count"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.count("balance").alias("balance_count"),
    )
    .orderBy(F.col("status_group").desc())
)
