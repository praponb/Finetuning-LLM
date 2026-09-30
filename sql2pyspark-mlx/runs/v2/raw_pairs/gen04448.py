from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(
        F.max("balance").alias("max_balance"),
        F.avg("balance").alias("avg_balance"),
        F.count("*").alias("num_accounts"),
        F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_count"),
    )
    .filter(F.col("frozen_count") > 1)
    .drop("frozen_count")
)
