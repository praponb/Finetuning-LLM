from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("cost") >= 550) & (F.col("category") == "Fashion"))
    .groupBy("brand")
    .agg(F.round(F.stddev("cost"), 2).alias("stddev_cost"))
    .orderBy(F.col("stddev_cost").desc(), F.col("brand").desc())
)
