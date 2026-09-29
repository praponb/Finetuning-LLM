from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") != "East")
    .groupBy("region")
    .agg(F.sum(F.when(F.col("city") == "Tokyo", 1).otherwise(0)).alias("tokyo_count"))
)
