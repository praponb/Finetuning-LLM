from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["Berlin", "Chiang Mai"]) & F.col("sqft").between(9701, 11174) & (F.col("sqft") != 11286))
    .groupBy("city")
    .agg(
        F.max("sqft").alias("max_sqft"),
        F.sum(F.when(F.col("region") == "Central", 1).otherwise(0)).alias("central_count"),
    )
)
