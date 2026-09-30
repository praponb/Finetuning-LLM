from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("status").isin(["CLOSED", "OPEN"]) & F.col("status").isNotNull())
    .select(
        "opened_date",
        "customer_id",
        "status",
        F.concat_ws(",", "account_type", "status").alias("account_type_status_key"),
    )
    .orderBy("status", "customer_id")
)
