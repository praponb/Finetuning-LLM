from pyspark.sql import functions as F

src = df_stores.filter(F.col("region") == "East")

agg = (
    src
    .groupBy("city", "region")
    .agg(F.min("sqft").alias("min_sqft"))
)

result = (
    agg
    .filter(F.col("min_sqft") > 15700)
    .select("city", "min_sqft")
    .orderBy(F.col("min_sqft").desc(), "city")
)
