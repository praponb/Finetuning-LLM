from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([10, 35, 40]))
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        F.count("balance").alias("balance_count"),
    )
)
