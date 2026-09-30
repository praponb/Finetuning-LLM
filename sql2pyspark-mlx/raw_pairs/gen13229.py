from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "id",
        "status",
        "opened_date",
        F.concat("status", F.lit(", "), "account_type").alias("status_account_type"),
    )
)
