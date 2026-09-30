from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region", "city")
    .agg(
        F.max("sqft").alias("max_sqft"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") >= 3)
    .select("region", "city", "max_sqft", "row_count")
)
