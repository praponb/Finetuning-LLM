from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["LOAN", "CREDIT"]))
    .select(
        "opened_date",
        "balance",
        "customer_id",
        F.concat_ws("-", "account_type", "status").alias("account_type_status_key"),
        F.trim("status").alias("clean_status"),
    )
    .orderBy(F.col("customer_id").desc())
)
