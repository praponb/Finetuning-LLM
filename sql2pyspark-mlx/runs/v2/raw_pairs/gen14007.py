from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") <= 2.9)
    .groupBy("city")
    .agg(F.round(F.stddev("rating"), 2).alias("stddev_rating"))
)
