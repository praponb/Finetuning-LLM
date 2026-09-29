from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") <= 2.4)
    .groupBy("city")
    .agg(F.round(F.stddev("rating"), 2).alias("stddev_rating"))
    .filter(F.col("stddev_rating") < 71)
    .select("city", "stddev_rating")
    .orderBy(F.col("stddev_rating").desc())
)
