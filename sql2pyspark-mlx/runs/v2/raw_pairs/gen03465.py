from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(((F.col("customer_id") == 23) & (F.col("balance") >= 41000)) | (F.col("balance") <= 58000))
    .select(
        "opened_date",
        "customer_id",
        "status",
        F.last_day("opened_date").alias("opened_date_month_end"),
    )
    .orderBy("customer_id")
)
