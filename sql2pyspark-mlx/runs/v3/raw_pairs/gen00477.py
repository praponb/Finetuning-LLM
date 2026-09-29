from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "id",
        "account_type",
        "customer_id",
        "status",
        F.substring("account_type", 1, 2).alias("account_type_prefix"),
    )
)
