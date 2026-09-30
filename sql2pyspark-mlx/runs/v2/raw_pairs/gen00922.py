from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~((F.col("opened_date") < "2024-09-01") | F.col("account_type").isin(["CHECKING", "CREDIT"])))
    .select(
        "balance",
        "customer_id",
        F.length("status").alias("status_length"),
        F.last_day("opened_date").alias("opened_date_month_end"),
    )
    .orderBy(F.col("status_length").desc(), F.col("customer_id").desc())
)
