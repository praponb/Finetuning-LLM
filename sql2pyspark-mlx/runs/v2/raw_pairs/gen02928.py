from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "id",
        "balance",
        "opened_date",
        F.date_sub("opened_date", 10).alias("opened_date_minus_10d"),
    )
    .orderBy("opened_date")
)
