from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "account_type",
        F.date_trunc("YEAR", "opened_date").alias("opened_date_period"),
        F.coalesce("balance", F.lit(0)).alias("balance_filled"),
    )
)
