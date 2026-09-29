from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.count("balance").alias("balance_count"),
    )
    .filter(F.col("balance_count") < 9)
    .select("opened_month", "min_opened_date", "balance_count")
)
