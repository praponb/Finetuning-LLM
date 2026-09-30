from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id") == 32)
    .groupBy(
        "account_type",
        F.date_format("opened_date", "yyyy-MM").alias("opened_month"),
    )
    .agg(
        F.max("balance").alias("max_balance"),
        F.count("balance").alias("balance_count"),
        F.min("opened_date").alias("min_opened_date"),
    )
    .filter(F.col("balance_count") < 1)
)
