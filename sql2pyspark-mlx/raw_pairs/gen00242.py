from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.quarter("opened_date").alias("opened_quarter"), "customer_id")
    .agg(
        F.sum("balance").alias("total_balance"),
        F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_count"),
        F.countDistinct("balance").alias("unique_statuss"),
    )
    .filter(F.col("unique_statuss") >= 2)
    .orderBy("unique_statuss", F.col("frozen_count").desc())
)
