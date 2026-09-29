from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 13737, "high").when(F.col("sqft") >= 7538, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg((F.max("sqft") - F.min("sqft")).alias("sqft_range"))
    .orderBy("sqft_bucket")
)
