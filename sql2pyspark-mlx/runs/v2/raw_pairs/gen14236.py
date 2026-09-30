from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "id",
        "balance",
        F.weekofyear("opened_date").alias("opened_date_weekofyear"),
        F.concat_ws(",", "account_type", "status").alias("account_type_status_key"),
    )
)
