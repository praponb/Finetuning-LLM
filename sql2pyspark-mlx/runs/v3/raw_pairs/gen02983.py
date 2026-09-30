from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(
        F.count("balance").alias("balance_count"),
        F.min("opened_date").alias("min_opened_date"),
    )
    .filter(F.col("balance_count") >= 8)
    .select("account_type", "balance_count", "min_opened_date")
)
