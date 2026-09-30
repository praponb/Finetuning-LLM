from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "JP")
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("city") == "London", F.col("rating")).otherwise(0)).alias("london_rating"),
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
    )
    .orderBy("country")
)
