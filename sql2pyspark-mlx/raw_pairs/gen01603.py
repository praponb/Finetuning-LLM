from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.month("opened_date").isin([1, 2, 9]) & F.month("opened_date").isin([6, 7, 12]))
    .select(
        "id",
        "status",
        "account_type",
        "balance",
        F.last_day("opened_date").alias("opened_date_month_end"),
        F.date_format("opened_date", "yyyy-MM-dd").alias("opened_date_label"),
    )
    .orderBy("status", "id")
    .limit(50)
)
