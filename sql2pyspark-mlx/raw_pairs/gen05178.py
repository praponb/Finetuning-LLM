from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["CREDIT", "LOAN", "SAVINGS"]))
    .select(
        "customer_id",
        "account_type",
        F.coalesce("status", F.lit("N/A")).alias("status_filled"),
        F.last_day("opened_date").alias("opened_date_month_end"),
    )
)
