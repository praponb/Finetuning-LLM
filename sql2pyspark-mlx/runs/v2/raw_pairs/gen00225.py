from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("status") == "OPEN", 1).otherwise(0)).alias("open_count"),
        F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_count"),
    )
    .orderBy(F.col("account_type").desc())
)
