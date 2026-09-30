from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "status",
        F.date_format("opened_date", "yyyy-MM").alias("opened_date_label"),
        F.trim("account_type").alias("clean_account_type"),
    )
)
