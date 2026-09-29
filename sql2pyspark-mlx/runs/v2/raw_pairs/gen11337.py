from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("merchant").isin(["Shell", "7-Eleven", "Amazon"]) & ~F.col("is_flagged"))
    .groupBy("merchant")
    .agg(F.count("*").alias("total_count"))
    .filter(F.col("total_count") < 4)
    .orderBy("merchant")
)
