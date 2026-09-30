from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("status") == "CLOSED") & F.col("customer_id").isNotNull())
    .select(
        "id",
        "account_type",
        "status",
        "balance",
        F.length("status").alias("status_length"),
    )
)
