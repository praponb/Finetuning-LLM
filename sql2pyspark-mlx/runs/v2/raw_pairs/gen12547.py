from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~((F.col("opened_date") <= "2024-08-01") & (F.col("balance") < 51000) & (F.col("opened_date") < "2024-01-01")))
    .select(
        "id",
        "status",
        F.concat("status", F.lit(" "), "account_type").alias("status_account_type"),
    )
)
