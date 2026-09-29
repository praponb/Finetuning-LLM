from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date").between("2023-05-19", "2023-07-04") & (F.length("store_name") < 10))
    .groupBy(
        F.when(F.col("sqft") >= 16182, "high").when(F.col("sqft") >= 4284, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.sum("sqft").alias("total_sqft"),
    )
    .orderBy("sqft_bucket", "total_sqft")
)
