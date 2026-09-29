from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") == 12359) & (F.col("opened_date") > "2023-04-01"))
    .groupBy(
        F.when(F.col("sqft") >= 15995, "high").when(F.col("sqft") >= 7343, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.max("opened_date").alias("last_opened_date"),
        F.countDistinct("sqft").alias("unique_regions"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("total_count") < 1)
    .orderBy(F.col("sqft_bucket").desc())
)
