from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("city") == "Berlin", F.col("sqft")).otherwise(0)).alias("berlin_sqft"),
    )
    .filter(F.col("berlin_sqft") < 47)
    .orderBy("city")
)
