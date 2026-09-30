from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") == "FROZEN")
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        F.add_months("opened_date", 1).alias("opened_date_plus_months"),
        F.coalesce("balance", F.lit(0)).alias("balance_filled"),
    )
)
