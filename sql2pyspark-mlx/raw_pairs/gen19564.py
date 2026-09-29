from pyspark.sql import functions as F

recent_accounts = (
    df_accounts
    .filter((F.col("account_type") == "SAVINGS") & (F.col("opened_date") < "2023-04-21"))
)

summary = (
    recent_accounts
    .groupBy("account_type", "customer_id")
    .agg(
        F.max("opened_date").alias("last_opened_date"),
        F.count(F.when(F.col("account_type") == "SAVINGS", 1)).alias("savings_count"),
    )
)

result = summary
