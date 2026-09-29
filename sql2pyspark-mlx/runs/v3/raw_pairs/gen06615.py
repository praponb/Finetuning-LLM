from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") > "2023-07-01")
    .select(
        "customer_id",
        "status",
        "account_type",
        F.coalesce("balance", F.lit(0)).alias("balance_filled"),
    )
    .orderBy("balance_filled", "status")
)
