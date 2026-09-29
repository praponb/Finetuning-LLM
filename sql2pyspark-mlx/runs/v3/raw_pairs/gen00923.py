from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .agg(
        F.countDistinct("rating").alias("unique_citys"),
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
    )
)
