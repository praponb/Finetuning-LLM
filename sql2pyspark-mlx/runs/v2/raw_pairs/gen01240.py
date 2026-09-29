from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("city") == "Berlin") & F.col("region").isin(["North", "East"]))
    .groupBy("city", "region")
    .agg(F.min("sqft").alias("min_sqft"))
    .filter(F.col("min_sqft") >= 14600)
)
