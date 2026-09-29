from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        F.max("opened_date").alias("last_opened_date"),
        F.count("balance").alias("balance_count"),
    )
    .filter(F.col("balance_count") < 8)
    .select("status", "last_opened_date", "balance_count")
)
