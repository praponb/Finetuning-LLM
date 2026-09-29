from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city", "region")
    .agg(
        F.sum(F.when(F.col("city") == "Singapore", F.col("sqft")).otherwise(0)).alias("singapore_sqft"),
    )
    .orderBy("singapore_sqft", "region")
)
