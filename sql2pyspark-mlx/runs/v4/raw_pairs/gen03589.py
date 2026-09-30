from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
        F.min("sqft").alias("min_sqft"),
    )
    .filter(F.col("min_sqft") >= 8600)
    .select("region", "sqft_range", "min_sqft")
    .orderBy(F.col("region").desc())
)
