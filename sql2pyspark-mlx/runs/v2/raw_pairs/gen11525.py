from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2025-04-01", "2025-08-01"))
    .select(
        "customer_id",
        "balance",
        "account_type",
        F.ceil("balance").alias("balance_whole"),
        F.concat_ws("-", "account_type", "status").alias("account_type_status_key"),
    )
)
