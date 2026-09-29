from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.month("opened_date").isin([1, 6, 10]))
    .groupBy("customer_id", "account_type")
    .agg(
        F.count("balance").alias("balance_count"),
        F.min("opened_date").alias("min_opened_date"),
    )
)
