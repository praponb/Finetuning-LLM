from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("store_name").like("%Downtow%") & F.col("city").isin(["New York", "Bangkok"])) | F.col("store_name").like("%Cen%"))
    .groupBy(
        F.month("opened_date").alias("opened_month"),
        F.when(F.col("sqft") >= 13640, "high").when(F.col("sqft") >= 6569, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.countDistinct("sqft").alias("unique_citys"),
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count"),
        F.round(((100 * F.sum(F.when(F.col("city") == "London", 1).otherwise(0))) / F.count("*")), 1).alias("pct_london"),
    )
    .filter(F.col("unique_citys") < 5)
)
