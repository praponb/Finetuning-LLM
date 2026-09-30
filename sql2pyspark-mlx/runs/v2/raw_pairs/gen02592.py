from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2024-06-01", "2025-04-01") & (F.col("balance") <= 31000) & (F.col("customer_id") == 34))
    .select(
        "id",
        "account_type",
        F.last_day("opened_date").alias("opened_date_month_end"),
    )
    .orderBy(F.col("opened_date_month_end").desc())
)
