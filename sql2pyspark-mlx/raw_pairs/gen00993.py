from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.1, "high").when(F.col("rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
        F.when(F.col("city").isin(["New York", "Bangkok"]), "tier_1").otherwise("other").alias("city_group"),
    )
    .agg(
        F.countDistinct("rating").alias("unique_citys"),
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
    )
)
