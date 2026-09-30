from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("city") == "Berlin") & F.col("store_name").like("%Downto%") & F.month("opened_date").isin([8, 9, 10]))
    .groupBy(
        F.when(F.col("sqft") >= 13093, "high").when(F.col("sqft") >= 8034, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.countDistinct("sqft").alias("unique_regions"),
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_count"),
        F.count("sqft").alias("sqft_count"),
    )
    .orderBy("unique_regions")
)
