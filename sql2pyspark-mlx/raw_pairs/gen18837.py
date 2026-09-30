from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.size("tags") >= 2)
    .groupBy("category")
    .agg(F.round(F.stddev("price"), 2).alias("stddev_price"))
    .filter(F.col("stddev_price") < 26)
    .select("category", "stddev_price")
    .orderBy("category")
)
