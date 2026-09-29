from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "status",
        "balance",
        F.substring("account_type", 1, 3).alias("account_type_prefix"),
    )
    .orderBy("account_type_prefix")
)
