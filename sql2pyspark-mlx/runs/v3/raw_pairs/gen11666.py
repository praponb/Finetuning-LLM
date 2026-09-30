from pyspark.sql import functions as F

base = (
    df_stores
    .filter((F.col("sqft") <= 7354) & F.col("sqft").isNotNull())
    .withColumn("clean_region", F.trim("region"))
)

summary = (
    base
    .groupBy("region")
    .agg(F.sum("sqft").alias("total_sqft"))
)

result = (
    summary
    .filter(F.col("total_sqft") < 80000)
    .select("region", "total_sqft")
    .orderBy(F.col("total_sqft").desc(), "region")
    .limit(3)
)
