from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .agg(
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
        F.sum(F.when(F.col("country") == "UK", 1).otherwise(0)).alias("uk_count"),
    )
    .filter(F.col("stddev_rating") < 76)
)
