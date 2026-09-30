from pyspark.sql import functions as F

result = (
    df_stores
    .agg(
        F.sum(F.when(F.col("region") == "South", 1).otherwise(0)).alias("south_count"),
        F.max("sqft").alias("max_sqft"),
    )
)
