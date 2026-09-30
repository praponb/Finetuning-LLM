from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & (F.col("customer_id").isNotNull() | (F.col("status") != "CLOSED")))
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.count("balance").alias("balance_count"),
    )
)
