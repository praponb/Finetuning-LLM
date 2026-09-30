from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .agg(
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
        F.count("rating").alias("rating_count"),
    )
)
