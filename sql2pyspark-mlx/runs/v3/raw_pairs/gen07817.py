from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "status",
        "customer_id",
        "opened_date",
        F.initcap(F.trim("status")).alias("status_title"),
        F.trim("account_type").alias("clean_account_type"),
    )
)
