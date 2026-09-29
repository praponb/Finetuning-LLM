from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("customer_id") == 39) & (F.col("opened_date") < "2024-02-01"))
    .select(
        "id",
        "customer_id",
        F.concat_ws("-", "account_type", "status").alias("account_type_status_key"),
    )
    .orderBy("account_type_status_key")
)
