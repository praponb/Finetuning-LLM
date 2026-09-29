from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.date_trunc("MONTH", "opened_date").alias("opened_month"),
        F.year("opened_date").alias("opened_year"),
        "customer_id",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
    )
)
