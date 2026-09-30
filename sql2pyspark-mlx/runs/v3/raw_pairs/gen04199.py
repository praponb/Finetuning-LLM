from pyspark.sql import functions as F

recent_stores = (
    df_stores
    .filter(F.col("city").isin(["Berlin", "London"]) | (F.col("opened_date") >= "2024-11-15"))
)

summary = (
    recent_stores
    .groupBy("city")
    .agg(
        F.min("sqft").alias("min_sqft"),
        F.max("sqft").alias("max_sqft"),
    )
)

result = (
    summary
    .filter(F.col("min_sqft") > 10600)
    .select("city", "min_sqft", "max_sqft")
    .orderBy("city")
)
