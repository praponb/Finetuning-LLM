from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "id",
        "account_type",
        F.upper("account_type").alias("account_type_upper"),
        F.concat_ws("-", "status", "account_type").alias("status_account_type_key"),
    )
    .orderBy("account_type", "account_type_upper")
)
