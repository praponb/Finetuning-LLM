from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("status") == "FROZEN") | (F.col("account_type") == "LOAN"))
    .select(
        "id",
        "opened_date",
        "status",
        "account_type",
        F.concat("account_type", F.lit(" - "), "status").alias("account_type_status"),
    )
)
