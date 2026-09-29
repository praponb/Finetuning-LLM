from pyspark.sql import functions as F

base = df_stores.filter(F.col("region").isin(["East", "West", "Central"]))

summary = (
    base
    .groupBy(
        F.when(F.col("sqft") >= 15834, "high").when(F.col("sqft") >= 6124, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.min("opened_date").alias("first_opened_date"),
        F.avg("sqft").alias("avg_sqft"),
    )
)

result = summary.orderBy(F.col("avg_sqft").desc())
