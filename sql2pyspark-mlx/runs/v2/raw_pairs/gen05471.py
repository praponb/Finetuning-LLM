from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") == "North")
    .groupBy("region")
    .agg(
        F.count(F.when(F.col("region") == "North", 1)).alias("north_count"),
        F.count("*").alias("store_count"),
    )
    .orderBy("region")
)
