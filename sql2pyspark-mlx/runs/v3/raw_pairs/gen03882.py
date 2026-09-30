from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") < 22000)
    .select(
        "status",
        "balance",
        F.last_day("opened_date").alias("opened_date_month_end"),
    )
    .orderBy("opened_date_month_end")
)
