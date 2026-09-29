from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .groupBy("merchant")
    .agg(F.count("*").alias("total_count"))
)
