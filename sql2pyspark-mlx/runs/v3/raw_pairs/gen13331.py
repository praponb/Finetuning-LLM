from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.year("opened_date") == 2025)
    .select(
        "id",
        "balance",
        "customer_id",
        "status",
        F.concat("status", F.lit(" "), "account_type").alias("status_account_type"),
        F.col("balance").cast("int").alias("balance_int"),
    )
)
