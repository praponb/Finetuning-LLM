from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("status") != "FROZEN") & (F.col("account_type") == "CHECKING") & (F.col("opened_date") >= "2025-05-12"))
    .select(
        "customer_id",
        "account_type",
        "balance",
        F.concat("status", F.lit("/"), "account_type").alias("status_account_type"),
        F.add_months("opened_date", 12).alias("opened_date_plus_months"),
    )
)
