from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(F.count("*").alias("total_count"))
    .filter(F.col("total_count") < 6)
    .orderBy(F.col("total_count").desc())
)
