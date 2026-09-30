from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isNull())
    .groupBy("region")
    .agg(F.sum(F.when(F.col("region") == "West", 1).otherwise(0)).alias("west_count"))
    .filter(F.col("west_count") >= 49)
    .select("region", "west_count")
)
