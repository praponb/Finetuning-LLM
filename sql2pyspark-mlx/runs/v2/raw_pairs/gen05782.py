from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        F.concat_ws("/", "status", "account_type").alias("status_account_type_key"),
        F.length("account_type").alias("account_type_length"),
    )
)
