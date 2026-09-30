from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "status",
        "account_type",
        F.last_day("opened_date").alias("opened_date_month_end"),
    )
)
