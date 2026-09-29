from pyspark.sql import functions as F

src = df_stores.filter(F.col("store_name").like("%Central%"))

summary = (
    src
    .groupBy(
        F.when(F.col("sqft") >= 14852, "high").when(F.col("sqft") >= 7174, "mid").otherwise("low").alias("sqft_bucket"),
        F.year("opened_date").alias("opened_year"),
    )
    .agg(
        F.sum("sqft").alias("total_sqft"),
        F.max("sqft").alias("max_sqft"),
    )
)

result = (
    summary
    .select("sqft_bucket", "opened_year", "total_sqft")
    .orderBy(F.col("total_sqft").desc(), "sqft_bucket", "opened_year")
    .limit(3)
)
