from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        F.count("*").alias("total_count"),
        F.max("balance").alias("max_balance"),
    )
    .orderBy("status")
)
