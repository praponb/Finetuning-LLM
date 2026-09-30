from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") != "Berlin")
    .groupBy("region")
    .pivot("city", ["Singapore", "Bangkok"])
    .agg(F.sum("sqft"))
    .orderBy("region")
)
