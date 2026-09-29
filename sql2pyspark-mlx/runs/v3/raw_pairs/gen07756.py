from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("customer_id") == 10) | (F.col("status") != "OPEN"))
    .select(
        "id",
        "balance",
        "opened_date",
        F.concat("account_type", F.lit(", "), "status").alias("account_type_status"),
    )
)
