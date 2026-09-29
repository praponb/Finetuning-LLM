from pyspark.sql import functions as F

src = (
    df_stores
    .filter(F.col("region").isin(["North", "South", "East"]) & (F.col("sqft") <= 5427))
)

sqft_bucket_summary = (
    src
    .groupBy(
        F.when(F.col("sqft") >= 13514, "high").when(F.col("sqft") >= 6995, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.sort_array(F.collect_set("city")).alias("city_list"))
)

result = sqft_bucket_summary.orderBy("sqft_bucket")
