from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(26000, 50000) & (F.col("customer_id").isin([10, 15, 24, 37]) | (F.col("account_type") == "LOAN")))
    .select(
        "customer_id",
        "account_type",
        "status",
        F.last_day("opened_date").alias("opened_date_month_end"),
    )
)
