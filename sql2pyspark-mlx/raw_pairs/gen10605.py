from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(
        F.countDistinct("balance").alias("unique_statuss"),
        F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_count"),
        F.count("*").alias("row_count"),
    )
)
