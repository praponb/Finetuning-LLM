from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") >= 6990) & (F.col("region").isin(["South", "North", "Central"]) | F.col("sqft").between(13672, 14678)))
    .groupBy("city")
    .agg(F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_count"))
    .orderBy(F.col("tokyo_count").desc(), F.col("city").desc())
)
