from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.count("balance").alias("balance_count"),
    )
)
