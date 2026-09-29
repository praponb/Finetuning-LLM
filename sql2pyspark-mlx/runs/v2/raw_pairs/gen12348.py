from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id") == 40)
    .groupBy("customer_id", F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.sum(F.when(F.col("status") == "FROZEN", 1).otherwise(0)).alias("frozen_count"),
    )
    .filter(F.col("frozen_count") >= 65)
    .select("customer_id", "opened_quarter", "frozen_count")
)
