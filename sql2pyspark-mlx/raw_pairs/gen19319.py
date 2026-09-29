from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.max("opened_date").alias("latest_opened_date"),
        F.min("balance").alias("min_balance"),
    )
    .filter(F.col("min_balance") >= 63000)
)
