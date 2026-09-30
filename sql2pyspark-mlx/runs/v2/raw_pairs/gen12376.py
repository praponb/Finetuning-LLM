from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isin([3, 12, 22, 30]) & F.col("store_id").isNotNull())
    .agg(F.count("*").alias("total_count"))
    .orderBy(F.col("total_count").desc())
)
