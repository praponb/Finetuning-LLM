from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") > 45000)
    .groupBy("status")
    .agg(
        F.max("opened_date").alias("last_opened_date"),
        F.sum(F.when(F.col("status") == "FROZEN", 1).otherwise(0)).alias("frozen_count"),
    )
    .filter(F.col("frozen_count") >= 21)
    .select("status", "last_opened_date", "frozen_count")
)
