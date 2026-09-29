from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("opened_date") < "2023-09-01") | (F.col("opened_date") >= "2023-04-01"))
    .groupBy(
        F.when(F.col("sqft") >= 17568, "high").when(F.col("sqft") >= 4794, "mid").otherwise("low").alias("sqft_bucket"),
        "city",
    )
    .agg(F.countDistinct("sqft").alias("unique_regions"))
    .filter(F.col("unique_regions") >= 1)
)
