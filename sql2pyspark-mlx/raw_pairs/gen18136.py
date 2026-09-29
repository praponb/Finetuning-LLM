from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") > 12303)
    .groupBy("city")
    .agg(
        F.count("sqft").alias("sqft_count"),
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
        F.sum(F.when(F.col("city") == "Bangkok", F.col("sqft")).otherwise(0)).alias("bangkok_sqft"),
    )
    .filter(F.col("bangkok_sqft") >= 73)
    .drop("bangkok_sqft")
)
