from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").isNotNull() & F.col("customer_id").isin([13, 16, 19, 24]))
    .select(
        "opened_date",
        "account_type",
        "customer_id",
        F.last_day("opened_date").alias("opened_date_month_end"),
    )
    .orderBy(F.col("customer_id").desc())
)
