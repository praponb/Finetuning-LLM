from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") <= 60000)
    .select(
        "account_type",
        "status",
        F.concat_ws(",", "account_type", "status").alias("account_type_status_key"),
    )
)
