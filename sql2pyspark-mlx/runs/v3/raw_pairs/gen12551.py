from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") < "2023-09-05")
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("status") == "FROZEN", 1).otherwise(0)).alias("frozen_count"),
        F.count("*").alias("total_count"),
    )
    .orderBy("status")
)
