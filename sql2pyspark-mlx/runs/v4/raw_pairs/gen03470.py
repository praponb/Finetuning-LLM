from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("account_type") == "LOAN") | (F.col("account_type") == "CREDIT"))
    .select(
        "id",
        "customer_id",
        "opened_date",
        F.substring("status", 1, 4).alias("status_prefix"),
    )
    .orderBy("id", "status_prefix")
)
