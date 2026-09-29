from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("opened_date") <= "2023-10-01") & F.col("customer_id").isNull())
    .select(
        "id",
        "opened_date",
        F.concat_ws("-", "account_type", "status").alias("account_type_status_key"),
    )
    .orderBy("opened_date", "id")
)
