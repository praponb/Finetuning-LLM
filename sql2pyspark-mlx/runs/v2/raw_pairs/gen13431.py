from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.count("*").alias("num_stores"),
        F.sum(F.when(F.col("region") == "Central", F.col("sqft")).otherwise(0)).alias("central_sqft"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("total_count") >= 1)
    .drop("total_count")
    .orderBy(F.col("num_stores").desc(), "region")
    .limit(5)
)
