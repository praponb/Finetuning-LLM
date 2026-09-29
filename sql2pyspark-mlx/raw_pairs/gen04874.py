from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") == "CLOSED")
    .groupBy(
        "status",
        F.date_trunc("MONTH", "opened_date").alias("opened_month"),
        F.year("opened_date").alias("opened_year"),
    )
    .agg(
        F.min("balance").alias("min_balance"),
        F.count("*").alias("num_accounts"),
    )
)
