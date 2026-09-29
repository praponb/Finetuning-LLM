from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") == "FROZEN")
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(
        F.sum(F.when(F.col("account_type") == "SAVINGS", 1).otherwise(0)).alias("savings_count"),
        F.round(((100 * F.sum(F.when(F.col("status") == "OPEN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_open"),
    )
    .filter(F.col("savings_count") >= 39)
)
