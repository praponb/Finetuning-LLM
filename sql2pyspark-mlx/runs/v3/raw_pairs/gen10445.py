from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") != "West")
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("city") == "Bangkok", F.col("sqft")).otherwise(0)).alias("bangkok_sqft"),
    )
    .filter(F.col("bangkok_sqft") < 80)
    .orderBy("bangkok_sqft")
)
