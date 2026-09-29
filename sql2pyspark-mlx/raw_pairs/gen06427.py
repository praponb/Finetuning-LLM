from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
        "region",
        F.when(F.col("sqft") >= 17000, "high").when(F.col("sqft") >= 6724, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.count("*").alias("total_count"),
        F.sum(F.when(F.col("region") == "West", F.col("sqft")).otherwise(0)).alias("west_sqft"),
        F.countDistinct("sqft").alias("unique_regions"),
    )
    .filter(F.col("unique_regions") > 1)
    .orderBy("unique_regions")
)
