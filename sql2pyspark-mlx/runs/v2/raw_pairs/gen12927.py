from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant")
    .agg(F.count("*").alias("row_count"))
    .orderBy(F.col("row_count").desc(), "merchant")
    .limit(5)
)
