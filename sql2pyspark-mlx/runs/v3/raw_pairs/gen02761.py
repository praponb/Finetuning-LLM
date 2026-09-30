from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.month("opened_date").isin([2, 4, 5]))
    .select(
        "id",
        "balance",
        "account_type",
        F.date_trunc("year", "opened_date").alias("opened_date_period"),
    )
)
