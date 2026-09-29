from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "id",
        "account_type",
        "customer_id",
        F.trim("status").alias("clean_status"),
        F.upper("account_type").alias("account_type_upper"),
    )
    .orderBy(F.col("clean_status").desc(), F.col("customer_id").desc())
)
