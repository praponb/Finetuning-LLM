from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") == "LOAN")
    .select(
        "customer_id",
        "balance",
        F.concat_ws(",", "status", "account_type").alias("status_account_type_key"),
        F.floor("balance").alias("balance_whole"),
    )
)
