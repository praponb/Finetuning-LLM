from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("opened_date") < "2023-07-01") & (F.col("customer_id").isin([4, 6, 24]) | (F.col("opened_date") > "2024-01-01")))
    .select(
        "id",
        "balance",
        "status",
        "opened_date",
        F.trim("account_type").alias("clean_account_type"),
    )
)
