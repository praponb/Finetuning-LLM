from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id") == 31)
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "FROZEN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_frozen"),
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
    )
    .filter(F.col("pct_frozen") < 72)
    .select("opened_month", "pct_frozen", "stddev_balance")
    .orderBy("opened_month")
)
