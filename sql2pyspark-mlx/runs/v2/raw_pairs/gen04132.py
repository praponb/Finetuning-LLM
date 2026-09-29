from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(
        F.min("sqft").alias("min_sqft"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("min_sqft") >= 9900)
    .orderBy(F.col("row_count").desc())
)
