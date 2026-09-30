from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["South", "West"]))
    .groupBy("region")
    .pivot("city", ["Singapore", "Tokyo", "New York"])
    .agg(F.count("sqft"))
    .orderBy("region")
)
