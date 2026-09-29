from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.sum(F.when(F.col("region") == "Central", F.col("sqft")).otherwise(0)).alias("central_sqft"),
    )
    .filter(F.col("central_sqft") < 65)
    .orderBy("central_sqft", "region")
    .limit(50)
)
