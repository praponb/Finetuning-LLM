from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(F.count("*").alias("total_count"))
    .filter(F.col("total_count") >= 3)
    .select("channel", "total_count")
    .orderBy("total_count", "channel")
)
