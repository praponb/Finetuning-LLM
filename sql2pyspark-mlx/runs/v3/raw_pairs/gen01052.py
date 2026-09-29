from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("city").isin(["Berlin", "New York"]), "tier_1").otherwise("other").alias("city_group"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0)).alias("bangkok_count"),
        F.round(((100 * F.sum(F.when(F.col("city") == "London", 1).otherwise(0))) / F.count("*")), 1).alias("pct_london"),
        F.countDistinct("sqft").alias("unique_citys"),
    )
    .orderBy(F.col("unique_citys").desc())
)
