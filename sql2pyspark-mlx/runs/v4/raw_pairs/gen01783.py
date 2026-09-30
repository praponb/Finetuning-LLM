from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("opened_date") > "2024-12-01") | (F.col("opened_date") >= "2024-04-01"))
    .agg(
        F.sum(F.when(F.col("status") == "FROZEN", 1).otherwise(0)).alias("frozen_count"),
        F.max("opened_date").alias("last_opened_date"),
    )
    .orderBy(F.col("frozen_count").desc())
)
