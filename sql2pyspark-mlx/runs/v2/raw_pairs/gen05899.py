from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "id",
        "opened_date",
        "account_type",
        "customer_id",
        F.date_sub("opened_date", 40).alias("opened_date_minus_40d"),
        F.substring("status", 1, 2).alias("status_prefix"),
    )
)
