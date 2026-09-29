from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") >= 4.2)
    .agg(F.round(F.stddev("rating"), 2).alias("stddev_rating"))
    .orderBy(F.col("stddev_rating").desc())
)
