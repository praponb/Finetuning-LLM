from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "id",
        "balance",
        "customer_id",
        F.upper("account_type").alias("account_type_upper"),
    )
)
