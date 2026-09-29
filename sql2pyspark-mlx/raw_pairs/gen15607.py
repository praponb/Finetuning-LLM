from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "customer_id",
        F.length("status").alias("status_length"),
        F.concat_ws("-", "status", "account_type").alias("status_account_type_key"),
    )
)
