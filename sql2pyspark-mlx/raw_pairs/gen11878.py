from pyspark.sql import functions as F

base = (
    df_stores
    .filter(F.col("region").isin(["West", "North", "South"]) & F.col("city").isin(["Singapore", "London", "Bangkok"]))
)

city_summary = (
    base
    .groupBy("city", "region")
    .agg(
        F.countDistinct("sqft").alias("unique_regions"),
        F.min("sqft").alias("min_sqft"),
    )
)

result = (
    city_summary
    .filter(F.col("unique_regions") > 4)
    .select("city", "region", "unique_regions", "min_sqft")
    .orderBy(F.col("unique_regions").desc(), F.col("city").desc(), "region")
    .limit(5)
)
