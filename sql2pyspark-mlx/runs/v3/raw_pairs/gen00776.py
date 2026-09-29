from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") <= 9685) | ~F.col("region").isin(["West", "North", "Central"]))
    .groupBy(
        "region",
        F.date_format("opened_date", "yyyy-MM").alias("opened_month"),
        F.when(F.col("sqft") >= 14294, "high").when(F.col("sqft") >= 4994, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.countDistinct("sqft").alias("unique_regions"),
        F.count("*").alias("store_count"),
    )
    .filter(F.col("store_count") >= 4)
    .orderBy("unique_regions")
)
