from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") > "2025-07-05")
    .select(
        "id",
        "account_type",
        "customer_id",
        "status",
        F.coalesce("balance", F.lit(0)).alias("balance_filled"),
    )
    .orderBy(F.col("balance_filled").desc())
)
