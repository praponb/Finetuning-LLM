from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") == "Books")
    .groupBy("brand")
    .agg(F.round(F.stddev("cost"), 2).alias("stddev_cost"))
    .orderBy("brand")
)
