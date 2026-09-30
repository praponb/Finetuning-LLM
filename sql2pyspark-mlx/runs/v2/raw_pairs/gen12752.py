from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") > 3.5) & (F.col("rating") > 3.1) & (F.col("rating") > 2.2))
    .groupBy("city")
    .agg(F.round(F.stddev("rating"), 2).alias("stddev_rating"))
    .filter(F.col("stddev_rating") >= 50)
)
