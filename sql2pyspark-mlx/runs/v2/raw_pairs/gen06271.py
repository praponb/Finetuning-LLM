from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(F.round(F.stddev("rating"), 2).alias("stddev_rating"))
    .orderBy(F.col("stddev_rating").desc(), "country")
    .limit(5)
)
