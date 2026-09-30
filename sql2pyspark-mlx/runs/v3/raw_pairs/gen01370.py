from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") > 2.5) & (F.col("city") == "Bangkok"))
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("country") == "UK", F.col("rating")).otherwise(0)).alias("uk_rating"),
        F.countDistinct("rating").alias("unique_citys"),
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
    )
)
