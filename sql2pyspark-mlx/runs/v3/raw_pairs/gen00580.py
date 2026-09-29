from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_count"),
        F.max("opened_date").alias("latest_opened_date"),
    )
    .filter(F.col("closed_count") < 5)
    .orderBy("status")
)
