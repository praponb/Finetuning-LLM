from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city", "country")
    .agg(
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count"),
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
    )
    .filter(F.col("stddev_rating") > 53)
    .orderBy("london_count", "stddev_rating", "city", "country")
    .limit(5)
)
