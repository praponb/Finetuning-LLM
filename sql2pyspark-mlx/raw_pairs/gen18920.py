from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "id",
        "account_type",
        "balance",
        "customer_id",
        F.last_day("opened_date").alias("opened_date_month_end"),
    )
)
