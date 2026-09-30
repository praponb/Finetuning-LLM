from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["CLOSED", "FROZEN"]) | F.month("opened_date").isin([1, 7, 12]))
    .select(
        "id",
        "account_type",
        "balance",
        "customer_id",
        F.trim("status").alias("clean_status"),
    )
)
