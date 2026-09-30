from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        F.concat("account_type", F.lit(" "), "status").alias("account_type_status"),
    )
)
