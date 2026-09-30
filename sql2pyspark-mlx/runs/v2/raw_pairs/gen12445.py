from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("customer_id") == 30) & (F.col("balance") < 42000) & (F.col("balance") >= 26000))
    .select(
        "id",
        "status",
        "opened_date",
        F.ceil("balance").alias("balance_whole"),
        F.concat_ws(",", "account_type", "status").alias("account_type_status_key"),
    )
)
