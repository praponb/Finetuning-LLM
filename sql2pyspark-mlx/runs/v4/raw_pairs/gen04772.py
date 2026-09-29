from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("customer_id") == 21) & (F.col("status") == "OPEN") & F.col("balance").isNull())
    .select(
        "id",
        "customer_id",
        "status",
        F.substring("account_type", 1, 2).alias("account_type_prefix"),
    )
)
