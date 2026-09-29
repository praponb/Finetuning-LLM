from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") >= "2025-03-01")
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.countDistinct("balance").alias("unique_statuss"),
        F.sum("balance").alias("total_balance"),
        F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_count"),
    )
    .orderBy(F.col("unique_statuss").desc(), "opened_quarter")
    .limit(1)
)
