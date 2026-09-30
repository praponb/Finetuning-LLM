from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Singapore", "London"]))
    .groupBy("country")
    .agg(F.round(F.stddev("rating"), 2).alias("stddev_rating"))
    .filter(F.col("stddev_rating") > 54)
    .select("country", "stddev_rating")
    .orderBy("stddev_rating", "country")
)
